package publisher 

import(
	"time"
	"sync"
	"fmt"
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/xyn3x/stockflow/pkg/model"
	"github.com/xyn3x/stockflow/pkg/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type Config struct {
	URL 			string 
	ConnectTimeout 	time.Duration 
	MaxReconnects 	int 

	StreamName		string 
	StreamSubjects 	[]string 
	StreamMaxAge	time.Duration 
	StreamMaxBytes	int64

	BatchSize		int 
	FlushTimeout	time.Duration
}

type Publisher struct {
	cfg 	Config 
	log 	*zap.Logger 
	js 		jetstream.JetStream 
	nc 		*nats.Conn
	m 		*metrics.Metrics

	mu 		sync.Mutex 
	batch	[]model.Event 
	flushCh	chan struct{}

	published 	atomic.Uint64  
	dropped 	atomic.Uint64 
}

func New(cfg Config, log *zap.Logger, m *metrics.Metrics) (*Publisher, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100 
	}

	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 50 * time.Millisecond
	}

	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second 
	}

	opts := []nats.Option {
		nats.Name("ingestion"),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Warn("Nats error: disconnected", zap.Error(err))
		}), 
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("Nats reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("Nats connection error in %s: %w", cfg.URL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("Jetstream connection error: %w", err)
	}

	p := &Publisher {
		cfg: 		cfg, 
		log: 		log, 
		js: 		js, 
		nc: 		nc, 
		m:			m,
		batch: 		make([]model.Event, 0, cfg.BatchSize),
		flushCh: 	make(chan struct{}, 1),
	}

	if err := p.ensureStream(context.Background()); err != nil {
		nc.Close()
		return nil, err 
	}

	log.Info("Nats publisher is ready", zap.String("url", cfg.URL), zap.String("stream", cfg.StreamName))
	return p, nil 
}

func (p *Publisher) ensureStream(ctx context.Context) error {
	maxAge := p.cfg.StreamMaxAge
	if maxAge == 0 {
		maxAge = 24 * time.Hour 
	}

	maxByte := p.cfg.StreamMaxBytes 
	if maxByte == 0 {
		maxByte = 1 << 30 
	}

	streamCfg := jetstream.StreamConfig {
		Name: 		p.cfg.StreamName, 
		Subjects: 	p.cfg.StreamSubjects, 
		Retention: 	jetstream.LimitsPolicy, 
		MaxAge: 	maxAge,
		MaxBytes: 	maxByte,
		Storage: 	jetstream.FileStorage,
		Replicas: 	1, 
		Discard: 	jetstream.DiscardOld,
	}

	_, err := p.js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("Ensure Stream %q: %w", p.cfg.StreamName, err)
	}
	p.log.Info("Stream is ready", zap.String("name", p.cfg.StreamName), zap.Strings("subjects", p.cfg.StreamSubjects))
	return nil 
}

func (p *Publisher) Publish(e model.Event) {
	p.mu.Lock()
	p.batch = append(p.batch, e)
	isFull := len(p.batch) >= p.cfg.BatchSize
	p.mu.Unlock()

	if isFull {
		select {
		case p.flushCh <- struct{}{}:
		default:
		}
	}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.FlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
			defer cancel()
			p.flush(shutdownCtx)
			return 
		case <-ticker.C:
			p.flush(ctx)
		case <-p.flushCh:
			p.flush(ctx)
		}
	}
}

type pendingAck struct {
	id 			string 
	eventType 	model.EventType
	subject 	string 
	future 		jetstream.PubAckFuture
}

func (p *Publisher) flush(ctx context.Context) {
	p.mu.Lock()
	if len(p.batch) == 0 {
		p.mu.Unlock()
		return 
	}
	toFlush := p.batch
	p.batch = make([]model.Event, 0, p.cfg.BatchSize)
	p.mu.Unlock()
	
	pending := make([]pendingAck, 0, len(toFlush))
	
	for _, e := range toFlush {
		subj := subjectFor(e)
		data, err := json.Marshal(e)
		if err != nil {
			p.log.Error("Marshal event error", zap.String("id", e.ID), zap.Error(err))
			p.dropped.Add(1)
			p.m.EventsDropped.WithLabelValues("ingestion", "dropped").Inc()
			continue 
		}

		future, err := p.js.PublishAsync(subj, data)

		if err != nil {
			p.log.Error("Nats publish error", zap.String("subject", subj), zap.String("id", e.ID), zap.Error(err))
			p.dropped.Add(1)
			p.m.EventsDropped.WithLabelValues("ingestion", "dropped").Inc()
			continue 
		}
		pending = append(pending, pendingAck{id : e.ID, eventType : e.Type, subject : subj, future : future})
	}

	if len(pending) == 0 {
		return 
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5 * time.Second)
	defer cancel()

	select {
	case <-p.js.PublishAsyncComplete():
	case <-waitCtx.Done():
		p.log.Warn("Timed out for waiting batch acks", zap.Int("still_pending", p.js.PublishAsyncPending()))
	}

	for _, pa := range pending {
		select {
		case <-pa.future.Ok():
			p.published.Add(1)
			p.m.EventsTotal.WithLabelValues("ingestion", string(pa.eventType), "published").Inc()
		case err := <-pa.future.Err():
			p.log.Error("Nats publish ack error", zap.String("subject", pa.subject), zap.String("id", pa.id), zap.Error(err))
			p.dropped.Add(1)
			p.m.EventsDropped.WithLabelValues("ingestion", "ack_dropped").Inc()
		default: 
			p.log.Warn("Nats publish ack timeout", zap.String("subject", pa.subject), zap.String("id", pa.id))
			p.dropped.Add(1)
			p.m.EventsDropped.WithLabelValues("ingestion", "ack_timeout").Inc()
		}
	}
	if len(toFlush) > 0 {
		p.log.Debug("Flush Batch", 
			zap.Int("count", len(toFlush)), 
			zap.Uint64("total_dropped", p.dropped.Load()), 
			zap.Uint64("total_published", p.published.Load()))
	}
}

func subjectFor(e model.Event) string {
	return fmt.Sprintf("events.raw.%s", string(e.Type))
}

func (p *Publisher) Close() {
	if p.nc != nil {
		p.nc.Drain()
	}
}

func (p *Publisher) Stats() (published, dropped uint64) {
	return p.published.Load(), p.dropped.Load()
}