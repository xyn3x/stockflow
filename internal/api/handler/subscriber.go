package handler 

import(
	"fmt"
	"time"
	"encoding/json"
	"context"
	"sync"
	
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xyn3x/stockflow/internal/api/store"
	apiws "github.com/xyn3x/stockflow/internal/api/websocket"
	"github.com/xyn3x/stockflow/pkg/model"
	"github.com/xyn3x/stockflow/pkg/metrics"
	"go.uber.org/zap"
)

type ProcessedResult struct {
	EventID 	string 				`json:"event_id"`
	EventType 	model.EventType		`json:"event_type"`
	ProcessedAt	time.Time			`json:"processed_at"`
	Metrics 	map[string] any 	`json:"metrics"`
}

type Subscriber struct {
	log 		*zap.Logger 
	nc			*nats.Conn 
	js 			jetstream.JetStream 
	hub 		*apiws.Hub 
	store 		*store.Store 
	m			*metrics.Metrics
	streamName 		string 
	consumerName 	string 
	consumer 		jetstream.Consumer
	numWorkers 		int
}

type pendingPersist struct {
	msg jetstream.Msg 
	res ProcessedResult
}

func NewSubscriber(
	natsURL string,
	connectTimeout time.Duration, 
	streamName, consumerName string, 
	hub *apiws.Hub, 
	store *store.Store, 
	m *metrics.Metrics,
	log *zap.Logger, 
	numWorkers int,
) (*Subscriber, error) {
	opts := []nats.Option{
		nats.Name("api-gateaway"),
		nats.Timeout(connectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", zap.Error(err))
		}), 
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
	}

	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connection %s: %w", natsURL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream initilization: %w", err)
	}

	return &Subscriber{
		log: log, 
		nc: nc, 
		js: js, 
		hub: hub, 
		store: store, 
		m : m,
		streamName: streamName, 
		consumerName: consumerName,
		numWorkers:	numWorkers,
	}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	consumer, err := s.ensureConsumer(ctx)
	if err != nil {
		return err 
	}
	s.consumer = consumer 

	s.log.Info("api consumer started", zap.String("stream", s.streamName), zap.String("consumer", s.consumerName))

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return 
			case <-ticker.C:
				if info, err := s.consumer.Info(ctx); err == nil {
					s.m.NATSLag.Set(float64(info.NumPending))
				}
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			s.log.Info("api subscriber stopped")
			return nil 
		}

		msgs, err := consumer.Fetch(50, jetstream.FetchMaxWait(200 * time.Millisecond))
		if err != nil {
			if err == jetstream.ErrNoMessages {
				continue 
			}
			s.log.Warn("fetch error", zap.Error(err))
			continue 
		}

		msgCh := make(chan jetstream.Msg, 50)
		pendingCh := make(chan pendingPersist, 50)

		var wg sync.WaitGroup 
		for i := 0; i < s.numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for msg := range msgCh {
					if pp, ok := s.process(msg); ok {
						pendingCh <- pp 
					}
				}
			}()
		}

		for msg := range msgs.Messages() {
			msgCh <- msg 
		}
		close(msgCh)

		go func() {
			wg.Wait()
			close(pendingCh)
		}()

		pending := make([]pendingPersist, 0, 50)
		for pp := range pendingCh {
			pending = append(pending, pp)
		}

		s.persistBatch(ctx, pending)
	}
}

func (s *Subscriber) process(msg jetstream.Msg) (pendingPersist, bool) {
	var res ProcessedResult 
	if err := json.Unmarshal(msg.Data(), &res); err != nil {
		s.log.Error("unmarshal res", zap.Error(err))
		msg.Nak()
		return pendingPersist{}, false
	}

	s.log.Info(
        "received event",
        zap.String("type", string(res.EventType)),
        zap.Any("metrics", res.Metrics),
    )
	s.hub.Broadcast(res)

	return pendingPersist{msg: msg, res : res}, true
}

func (s *Subscriber) persistBatch(ctx context.Context, pending []pendingPersist) {
	if len(pending) == 0 {
		return 
	}

	items := make([]store.KV, 0, len(pending) * 2)
	for _, pp := range pending {
		items = append(items, s.kvFor(pp.res)...)
	}

	if err := s.store.BatchSet(ctx, items); err != nil {
		s.log.Warn("redis persist batch", zap.Error(err), zap.Int("batch_size", len(pending)))
		for _, pp := range pending {
			pp.msg.Nak()
		}
		return 
	}

	for _, pp := range pending {
		pp.msg.Ack()
	}
}
func (s *Subscriber) kvFor(r ProcessedResult) []store.KV {
	now := time.Now().UTC()

	switch r.EventType {
	case model.EventTypeStock: 
		ticker, _ := r.Metrics["ticker"].(string)
		if ticker == "" {
			return nil 
		}
		price, _ := r.Metrics["price"].(float64)
		avg, _ := r.Metrics["moving_avg"].(float64)
		vol, _ := r.Metrics["volatility"].(float64)
		topRaw, _ := r.Metrics["top_by_volume"].([]any)
		top := anyToStrings(topRaw)

		return []store.KV {
			{
				Key: store.TickerKey(ticker),
				Value: store.MetricSnapshot{
					Key: ticker, 
					Price: price, 
					MovingAvg: avg,
					Volatility: vol, 
					UpdatedAt: now, 
				},
			},
			{
				Key: store.TopKKey("stock"),
				Value: top,
			},
		}
		
	case model.EventTypeClick:
		topRaw, _ := r.Metrics["top_elements"].([]any)
		top := anyToStrings(topRaw)
		return []store.KV {
			{
				Key: store.TopKKey("element"),
				Value: top, 
			},
		}

	case model.EventTypeTelemetry:
		srv, _ := r.Metrics["service"].(string)
		metric, _ := r.Metrics["metric"].(string)
		if srv == "" || metric == "" {
			return nil 
		}
		avg, _ := r.Metrics["moving_avg"].(float64)
		vol, _ := r.Metrics["volatility"].(float64)
		val, _ := r.Metrics["value"].(float64)
		key := srv + "." + metric 

		return []store.KV {
			{
				Key: store.TelemetryKey(key), 
				Value: store.MetricSnapshot {
					Key: key, 
					MovingAvg: avg, 
					Volatility: vol, 
					UpdatedAt: now, 
					Extra: map[string] any{"value": val, "unit": r.Metrics["unit"]}, 
				},
			},
		}
		
	}
	return nil 
}

func (s *Subscriber) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	cfg := jetstream.ConsumerConfig {
		Name: 			s.consumerName, 
		Durable: 		s.consumerName, 
		FilterSubject: 	"events.processed",
		AckPolicy: 		jetstream.AckExplicitPolicy, 
		MaxDeliver: 	3, 
		AckWait: 		30 * time.Second,
	}
	consumer, err := s.js.CreateOrUpdateConsumer(ctx, s.streamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer %q: %w", s.consumerName, err)
	}
	return consumer, nil 
}

func (s *Subscriber) Close() {
	if s.nc != nil {
		s.nc.Drain()
	}
}

func anyToStrings(in []any) []string {
	out := make([]string, 0, len(in))
	for _, data := range in {
		if cur, ok := data.(string); ok {
			out = append(out, cur)
		}
	}
	return out
}