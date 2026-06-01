package store 

import(
	"context"
	"fmt"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const(
	keyLastResult 	= "result:%s"
	keyTickerState	= "ticker:%s"
	keyTopK 		= "topk:%s"
	keyTelemetry 	= "telemetry:%s"
	defaultTTL		= 5 * time.Minute
)

type MetricSnapshot struct {
	Key 		string 			`json:"key"`
	Price		float64			`json:"price,omitempty"`
	MovingAvg 	float64			`json:"moving_avg"`
	Volatility 	float64			`json:"volatility"`
	UpdatedAt	time.Time 		`json:"updated_at"`
	Extra		map[string] any `json:"extra,omitempty"`
}

type Store struct {
	rdb 	*redis.Client 
	ttl		time.Duration
}

func New(addr, password string, db int) *Store {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, 
		Password: password,
		DB: db, 
	})
	return &Store{rdb: rdb, ttl: defaultTTL}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

func (s *Store) SetTicker(ctx context.Context, ticker string, snap MetricSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snap error, ticker %s: %w", ticker, err)
	}
	return s.rdb.Set(ctx, fmt.Sprintf(keyTickerState, ticker), data, s.ttl).Err()
}
func (s *Store) GetTicker(ctx context.Context, ticker string) (*MetricSnapshot, error) {
	data, err := s.rdb.Get(ctx, fmt.Sprintf(keyTickerState, ticker)).Bytes()
	if err == redis.Nil {
		return nil, nil 
	}
	if err != nil {
		return nil, fmt.Errorf("didn't get a ticker %s: %w", ticker, err)
	}
	var snap MetricSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("json unmarshal ticker %s: %w", ticker, err)
	}
	return &snap, nil
}

func (s *Store) SetTopK(ctx context.Context, category string, entries []string) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal topk %s: %w", category, err)
	}
	return s.rdb.Set(ctx, fmt.Sprintf(keyTopK, category), data, s.ttl).Err()
}

func (s *Store) GetTopK(ctx context.Context, category string) ([]string, error) {
	data, err := s.rdb.Get(ctx, fmt.Sprintf(keyTopK, category)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("didn't get a topk %s: %w", category, err)
	}
	var entries []string 
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("json unmarshal topk %s: %w", category, err)
	}
	return entries, nil
}

func (s *Store) SetTelemetry(ctx context.Context, key string, snap MetricSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("json marshal telemetry %s: %w", key, err)
	}
	return s.rdb.Set(ctx, fmt.Sprintf(keyTelemetry, key), data, s.ttl).Err()
}

func (s *Store) GetTelemetry(ctx context.Context, key string) (*MetricSnapshot, error) {
	data, err := s.rdb.Get(ctx, fmt.Sprintf(keyTelemetry, key)).Bytes()
	if err == redis.Nil {
		return nil, nil 
	}
	if err != nil {
		return nil, fmt.Errorf("didn't get a telemetry %s: %w", key, err)
	}
	var snap MetricSnapshot 
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("json unmarshal telemetry %s: %w", key, err)
	}
	return &snap, nil 
}

func (s *Store) Close() error {
	return s.rdb.Close()
}