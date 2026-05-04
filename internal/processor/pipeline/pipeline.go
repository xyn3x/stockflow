package pipeline 

import(
	"encoding/json"
	"fmt"
	"time"

	"github.com/xyn3x/stockflow/internal/processor/aggregation"
	"github.com/xyn3x/stockflow/internal/processor/metrics"
	"github.com/xyn3x/stockflow/pkg/model"
	"go.uber.org/zap"
)

type Result struct {
	EventID 	string 				`json:"event_id"`	
	EventType 	model.EventType		`json:"event_type"`
	ProcessedAt time.Time 			`json:"processed_at"`
	Metrics 	map[string] any 	`json:"metrics"`
}

type Pipeline struct {
	log 		*zap.Logger 
	movingAvg 	*aggregation.MovingAverage 
	topK 		*aggregation.TopK 
	volatility 	*aggregation.Volatility 
	throughput 	*metrics.Throughput 
	latency 	*metrics.Latency
}

func New(windowSz, k int, log *zap.Logger) *Pipeline {
	return &Pipeline {
		log: 		log, 
		movingAvg: 	aggregation.NewMovingAverage(windowSz), 
		topK: 		aggregation.NewTopK(k), 
		volatility: aggregation.NewVolatility(), 
		throughput: metrics.NewThroughput(), 
		latency: 	metrics.NewLatency(),
	}
}

func (p *Pipeline) Process(event model.Event) (*Result, error) {
	start := time.Now()

	var m map[string] any 
	var err error 

	switch event.Type {
	case model.EventTypeStock:
		m, err = p.processStock(event)
	case model.EventTypeClick:
		m, err = p.processClick(event)
	case model.EventTypeTelemetry: 
		m, err = p.processTelemetry(event)
	default:
		return nil, fmt.Errorf("Unknown event type: %q", event.Type)
	}

	if err != nil {
		return nil, err 
	}

	elapsed := time.Since(start)
	p.latency.Record(elapsed)
	rate := p.throughput.Inc()
	m["processing_latency"] = elapsed.Microseconds()
	m["events_per_second"] = rate 

	return &Result {
		EventID: 		event.ID, 
		EventType: 		event.Type, 
		ProcessedAt: 	time.Now().UTC(), 
		Metrics: 		m, 
	}, nil
}

func (p *Pipeline) processStock(event model.Event) (map[string] any, error) {
	var payload model.StockPayload 
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, fmt.Errorf("json umarshal: encode stock payload: %w", err)
	}

	avg := p.movingAvg.Add(payload.Ticker, payload.Price)
	vol := p.volatility.Add(payload.Ticker, payload.Price)
	top := p.topK.Add(payload.Ticker, payload.Price)

	topTickers := make([]string, len(top))
	for pos, cur := range top {
		topTickers[pos] = cur.Key
	}
	
	return map[string] any {
		"ticker": payload.Ticker, 
		"price": payload.Price, 
		"moving_avg": avg, 
		"volatility": vol, 
		"top_by_volume": topTickers, 
	}, nil
}

func (p *Pipeline) processClick(event model.Event) (map[string] any, error) {
	var payload model.ClickPayload 
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, fmt.Errorf("json unmarshal: encode click payload: %w", err)
	}

	p.topK.Add("page:"+payload.PageURL, 1)
	top := p.topK.Add("element:"+payload.Element, 1)

	topElements := make([]string, len(top))
	for pos, cur := range top {
		topElements[pos] = cur.Key 
	}

	return map[string] any {
		"user_id": 		payload.UserID, 
		"page": 		payload.PageURL, 
		"element": 		payload.Element, 
		"top_elements": topElements, 
	}, nil 
}

func (p *Pipeline) processTelemetry(event model.Event) (map[string] any, error) {
	var payload model.TelemetryPayload 
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, fmt.Errorf("json unmarshal: encode telemetry payload: %w", err)
	}

	key := payload.ServiceName + "." + payload.MetricName 
	avg := p.movingAvg.Add(key, payload.Value)
	vol := p.volatility.Add(key, payload.Value)

	return map[string] any {
		"service": 		payload.ServiceName, 
		"metric": 		payload.MetricName, 
		"value": 		payload.Value, 
		"unit": 		payload.Unit,
		"moving_avg":	avg, 
		"volatility":	vol, 
	}, nil
}

func (p *Pipeline) Stats() (total uint64, avgLatency, maxLatency time.Duration) {
	total = p.throughput.Total()
	avgLatency, maxLatency = p.latency.Stats()
	return 
}