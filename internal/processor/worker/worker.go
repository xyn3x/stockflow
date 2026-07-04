package worker 

import(
	"context"
	"encoding/json"
	"time"
	"fmt"

	"github.com/xyn3x/stockflow/internal/processor/pipeline"
	"github.com/xyn3x/stockflow/pkg/model"
	"github.com/xyn3x/stockflow/pkg/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	NATSURL 		string 
	ConnectTimeout 	time.Duration
	StreamName		string 
	ConsumerName 	string 
	FilterSubject 	string 
	FetchBatch 		int 
	FetchTimeout 	time.Duration
}

type Worker struct {
	cfg 		Config 
	log 		*zap.Logger 
	pipeline	*pipeline.Pipeline 
	nc 			*nats.Conn
	js 			jetstream.JetStream 
	m			*metrics.Metrics
	consumer	jetstream.Consumer
}

func New(cfg Config, pl *pipeline.Pipeline, log *zap.Logger, m *metrics.Metrics) (*Worker, error) {
	if cfg.FetchBatch <= 0 {
		cfg.FetchBatch = 50 
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 200 * time.Millisecond
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second 
	}

	opts := []nats.Option {
		nats.Name("processor"),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", zap.Error(err))
		}), 
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnecting", zap.String("url", nc.ConnectedUrl()))
		}),
	}
	
	nc, err := nats.Connect(cfg.NATSURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("error: nats is not connected %s: %w", cfg.NATSURL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("error: jetstream is not initialized: %w", err)
	}

	return &Worker {
		cfg: 		cfg, 
		log: 		log, 
		pipeline: 	pl, 
		nc: 		nc, 
		js: 		js, 
		m:			m,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	consumer, err := w.ensureConsumer(ctx)
	if err != nil {
		return err 
	}
	w.consumer = consumer 

	w.log.Info("Processor worker started", 
		zap.String("stream", w.cfg.StreamName), 
		zap.String("consumer", w.cfg.ConsumerName),
		zap.String("filter", w.cfg.FilterSubject))

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return 
			case <-ticker.C:
				total, avgLat, maxLat := w.pipeline.Stats()
				w.log.Info("Processor stats", 
					zap.Uint64("total_processed", total),
					zap.Duration("avg_latency", avgLat),
					zap.Duration("max_latency", maxLat))

				if info, err := w.consumer.Info(ctx); err == nil {
					w.m.NATSLag.Set(float64(info.NumPending))
				}
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			w.log.Info("Processor worker stopped successfully")
			return nil 
		}
		
		msgs, err := consumer.Fetch(w.cfg.FetchBatch, jetstream.FetchMaxWait(w.cfg.FetchTimeout))
		if err != nil {
			if err == jetstream.ErrNoMessages {
				continue 
			}
			w.log.Warn("fetch error", zap.Error(err))
			continue 
		}
		for msg := range msgs.Messages() {
			w.handleMessage(msg)
		}
		if err := msgs.Error(); err != nil {
			w.log.Warn("messages error", zap.Error(err))
		}
	}
}

func (w *Worker) handleMessage(msg jetstream.Msg) {
	var event model.Event 
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		w.log.Warn("json unmarshal error", zap.Error(err))
		msg.Nak()
		return 
	}

	timer := prometheus.NewTimer(
		w.m.ProcessingTime.WithLabelValues("processor", string(event.Type)),
	)
	defer timer.ObserveDuration()

	res, err := w.pipeline.Process(event)
	if err != nil {
		w.log.Error("pipeline process", 
			zap.String("id", event.ID), 
			zap.String("type", string(event.Type)), 
			zap.Error(err))
		w.m.EventsDropped.WithLabelValues("processor", "pipeline_process_error").Inc()
		msg.Nak()
		return 
	}

	data, err := json.Marshal(res)
	if err == nil {
		pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w.js.Publish(pubCtx, "events.processed", data)
	}
	
	w.log.Debug("event processed", 
		zap.String("id", res.EventID),
		zap.String("type", string(res.EventType)), 
		zap.Any("metrics", res.Metrics))
	w.m.EventsTotal.WithLabelValues("processor", string(event.Type), "processed").Inc()
	msg.Ack()
}

func (w *Worker) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	consumerCfg := jetstream.ConsumerConfig {
		Name: 			w.cfg.ConsumerName, 
		Durable: 		w.cfg.ConsumerName, 
		FilterSubject: 	w.cfg.FilterSubject, 
		AckPolicy: 		jetstream.AckExplicitPolicy, 
		MaxDeliver: 	3, 
		AckWait: 		30 * time.Second, 
	}

	consumer, err := w.js.CreateOrUpdateConsumer(ctx, w.cfg.StreamName, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer %q on stream %q: %w", 
			w.cfg.ConsumerName, w.cfg.StreamName, err)
	}
	return consumer, nil
}

func (w *Worker) Close() {
	if w.nc != nil {
		w.nc.Drain()
	}
}
