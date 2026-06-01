package handler 

import(
	"fmt"
	"time"
	"encoding/json"
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xyn3x/stockflow/internal/api/store"
	apiws "github.com/xyn3x/stockflow/internal/api/websocket"
	"github.com/xyn3x/stockflow/pkg/model"
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
	streamName 		string 
	consumerName 	string 
}


func NewSubscriber(
	natsURL string,
	connectTimeout time.Duration, 
	streamName, consumerName string, 
	hub *apiws.Hub, 
	store *store.Store, 
	log *zap.Logger, 
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
		streamName: streamName, 
		consumerName: consumerName,
	}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	consumer, err := s.ensureConsumer(ctx)
	if err != nil {
		return err 
	}

	s.log.Info("api consumer started", zap.String("stream", s.streamName), zap.String("consumer", s.consumerName))

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

		for msg := range msgs.Messages() {
			s.handle(ctx, msg) 
		}
	}
}

func (s *Subscriber) handle(ctx context.Context, msg jetstream.Msg) {
	var res ProcessedResult 
	if err := json.Unmarshal(msg.Data(), &res); err != nil {
		s.log.Error("unmarshal res", zap.Error(err))
		msg.Nak()
		return 
	}

	s.hub.Broadcast(res)

	if err := s.persist(ctx, res); err != nil {
		s.log.Warn("redis persist", zap.Error(err))
	}
	msg.Ack()
}

func (s *Subscriber) persist(ctx context.Context, r ProcessedResult) error {
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

		if err := s.store.SetTicker(ctx, ticker, store.MetricSnapshot {
			Key: 		ticker, 
			Price: 		price, 
			MovingAvg: 	avg, 
			Volatility: vol, 
			UpdatedAt: 	now,
		}); err != nil {
			return err 
		}
		return s.store.SetTopK(ctx, "stock", top)
		
	case model.EventTypeClick:
		topRaw, _ := r.Metrics["top_elements"].([]any)
		top := anyToStrings(topRaw)
		return s.store.SetTopK(ctx, "element", top)

	case model.EventTypeTelemetry:
		srv, _ := r.Metrics["service"].(string)
		metric, _ := r.Metrics["metric"].(string)
		if srv == "" || metric == "" {
			return nil 
		}
		avg, _ := r.Metrics["moving_avg"].(float64)
		vol, _ := r.Metrics["volatility"].(float64)
		val, _ := r.Metrics["value"].(float64)

		return s.store.SetTelemetry(ctx, srv+"."+metric, store.MetricSnapshot {
			Key: srv + "." + metric, 
			MovingAvg: avg, 
			Volatility: vol, 
			UpdatedAt: now, 
			Extra: map[string]any{"value": val, "unit": r.Metrics["unit"]},
		})
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