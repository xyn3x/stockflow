package simulator 

import(
	"encoding/json"
	"math"
	"math/rand"
	"fmt"
	"time"
	"sync"

	"github.com/xyn3x/stockflow/pkg/model"
	"go.uber.org/zap"
	"github.com/google/uuid"
)

type stockState struct {
	ticker 		string
	price 		float64
	volatility 	float64
}

type Generator struct {
	log 	*zap.Logger
	mu 		sync.Mutex
	stocks 	[]*stockState
	users 	[]string 
	apps	[]string 
	seed 	*rand.Rand

	// probabilities
	stockWeight 	float64 
	clickWeight 	float64 
	telemetryWeight float64 
}

type GeneratorConfig struct {
	StockTickers 	[]string 
	ClickUsers 		int 
	TelemetryApps 	[]string
	StockRatio		float64 
	ClickRatio		float64 
	TelemetryRatio	float64 
}

func NewGenerator(cfg GeneratorConfig, log *zap.Logger) *Generator {
	stocks := make([]*stockState, len(cfg.StockTickers))

	for pos, tick := range cfg.StockTickers {
		stocks[pos] = &stockState {
			ticker: tick,
			price: seedPrice(tick),
			volatility: 0.015 + rand.Float64() * 0.025,
		}
	}

	users := make([]string, cfg.ClickUsers)
	for id := range users {
		users[id] = fmt.Sprintf("user-%05d", id + 1)
	}
	
	apps := cfg.TelemetryApps 
	
	return &Generator {
		log: 				log, 
		stocks: 			stocks, 
		users: 				users, 
		apps: 				apps, 
		seed: 				rand.New(rand.NewSource(time.Now().UnixNano())), 
		stockWeight: 		cfg.StockRatio, 
		clickWeight: 		cfg.ClickRatio, 
		telemetryWeight: 	cfg.TelemetryRatio,
	}
}

func (gen *Generator) Next() model.Event {
	gen.mu.Lock()
	defer gen.mu.Unlock()

	cur_event := gen.seed.Float64()
	switch {
	case cur_event < gen.stockWeight:
		return gen.StockEvent()
	case cur_event < gen.stockWeight + gen.clickWeight:
		return gen.ClickEvent()
	default:
		return gen.TelemetryEvent()
	}
}

func (gen *Generator) StockEvent() model.Event {
	st := gen.stocks[gen.seed.Intn(len(gen.stocks))]

	dt := 1.0 / (6.5 * 3600)
	drift := 0.0
	shock := st.volatility * math.Sqrt(dt) * gen.seed.NormFloat64()
	st.price *= math.Exp(drift + shock)

	spread := st.price * 0.0003

	payload := model.StockPayload {
		Ticker: 	st.ticker, 
		Price: 		round(st.price, 3), 
		Bid: 		round(st.price - spread, 3), 
		Ask: 		round(st.price + spread, 3), 
		Volume: 	int64(gen.seed.Intn(10000) + 100),
	}

	return gen.wrap(model.EventTypeStock, payload)
}

func (gen *Generator) ClickEvent() model.Event {
	pages := []string{"/home", "/dashboard", "/settings", "/checkout", "/products"}
	elements := []string{"btn", "nav-link", "card-header", "search-bar", "checkbox"}

	payload := model.ClickPayload {
		UserID: 	gen.users[gen.seed.Intn(len(gen.users))],
		SessionID: 	uuid.New().String(),
		PageURL: 	pages[gen.seed.Intn(len(pages))],
		Element: 	elements[gen.seed.Intn(len(elements))],
	}
	
	return gen.wrap(model.EventTypeClick, payload)
}

func (gen *Generator) TelemetryEvent() model.Event {
	metrics := []struct {
		name  string
		unit  string
		base  float64
		noise float64
	}{
		{"http.request_duration_ms", "ms", 45, 30},
		{"http.requests_per_second", "rps", 250, 80},
		{"memory.usage_bytes", "bytes", 256e6, 50e6},
		{"cpu.usage_percent", "percent", 35, 20},
		{"db.query_duration_ms", "ms", 8, 5},
	}

	m := metrics[gen.seed.Intn(len(metrics))]
	app := gen.apps[gen.seed.Intn(len(gen.apps))]

	value := m.base + gen.seed.NormFloat64()*m.noise
	if value < 0 {
		value = 0
	}

	payload := model.TelemetryPayload {
		ServiceName: app, 
		MetricName: m.name, 
		Value: value, 
		Unit: m.unit, 
		Labels: map[string]string{
			"env":    "production",
			"region": "asia-center-1",
		},
	}

	return gen.wrap(model.EventTypeTelemetry, payload)
}

func (gen *Generator) wrap(t model.EventType, payload interface{}) model.Event {
	raw, _ := json.Marshal(payload)

	return model.Event {
		ID: 		uuid.New().String(),
		TimeStamp: 	time.Now().UTC(),
		Source: 	"simulator", 
		Type: 		t, 
		Payload: 	raw, 
	}
}

func seedPrice(tick string) float64 {
	seeds := map[string]float64{
		"AAPL": 189.50, "MSFT": 415.20, "GOOGL": 175.00,
		"AMZN": 193.40, "TSLA": 185.00, "META": 530.00,
		"NVDA": 875.00, "BTC":  68000.0, "ETH":  3500.0,
	}

	if prc, ok := seeds[tick]; ok {
		return prc 
	}

	// guess 
	return 135.40
}

func round(v float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(v*pow) / pow
}
