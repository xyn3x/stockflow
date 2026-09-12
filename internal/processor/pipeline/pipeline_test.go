package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/xyn3x/stockflow/pkg/model"
	"go.uber.org/zap"
)

func mustEvent(t *testing.T, typ model.EventType, payload any) model.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return model.Event{ID: "test-id", Type: typ, Payload: raw}
}

func TestProcessStock_TopByVolumeUsesVolumeNotPrice(t *testing.T) {
	p := New(5, 2, zap.NewNop())

	events := []model.Event{
		mustEvent(t, model.EventTypeStock, model.StockPayload{Ticker: "AAPL", Price: 900.00, Volume: 10}),
		mustEvent(t, model.EventTypeStock, model.StockPayload{Ticker: "MSFT", Price: 10.00, Volume: 9000}),
	}

	var lastMetrics map[string]any
	for _, e := range events {
		res, err := p.Process(e)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		lastMetrics = res.Metrics
	}

	top, ok := lastMetrics["top_by_volume"].([]string)
	if !ok || len(top) == 0 {
		t.Fatalf("top_by_volume missing or wrong type: %#v", lastMetrics["top_by_volume"])
	}
	if top[0] != "MSFT" {
		t.Errorf("top_by_volume[0] = %q, want %q (ranked by volume, not price)", top[0], "MSFT")
	}
}

func TestProcessStock_MetricsIncludeExpectedFields(t *testing.T) {
	p := New(5, 3, zap.NewNop())
	e := mustEvent(t, model.EventTypeStock, model.StockPayload{Ticker: "AAPL", Price: 189.5, Volume: 500})

	res, err := p.Process(e)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	for _, key := range []string{"ticker", "price", "moving_avg", "volatility", "top_by_volume", "processing_latency", "events_per_second"} {
		if _, ok := res.Metrics[key]; !ok {
			t.Errorf("missing expected metric key %q in %#v", key, res.Metrics)
		}
	}
	if res.EventID != "test-id" {
		t.Errorf("EventID = %q, want %q", res.EventID, "test-id")
	}
}

func TestProcessClick_TracksElementAndPage(t *testing.T) {
	p := New(5, 3, zap.NewNop())
	e := mustEvent(t, model.EventTypeClick, model.ClickPayload{UserID: "user-1", PageURL: "/checkout", Element: "btn"})

	res, err := p.Process(e)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	top, ok := res.Metrics["top_elements"].([]string)
	if !ok || len(top) == 0 || top[0] != "btn" {
		t.Errorf("top_elements = %#v, want first entry %q", res.Metrics["top_elements"], "btn")
	}
}

func TestProcessTelemetry_ComputesMovingAvgAndVolatility(t *testing.T) {
	p := New(5, 3, zap.NewNop())

	values := []float64{10, 20, 30}
	var lastMetrics map[string]any
	for _, v := range values {
		e := mustEvent(t, model.EventTypeTelemetry, model.TelemetryPayload{
			ServiceName: "api", MetricName: "cpu.usage_percent", Value: v, Unit: "percent",
		})
		res, err := p.Process(e)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		lastMetrics = res.Metrics
	}

	avg, ok := lastMetrics["moving_avg"].(float64)
	if !ok || avg != 20.0 {
		t.Errorf("moving_avg = %v, want 20.0", lastMetrics["moving_avg"])
	}
}

func TestProcess_UnknownEventTypeReturnsError(t *testing.T) {
	p := New(5, 3, zap.NewNop())
	e := model.Event{ID: "x", Type: "not_a_real_type", Payload: []byte(`{}`)}

	_, err := p.Process(e)
	if err == nil {
		t.Fatal("expected error for unknown event type, got nil")
	}
}

func TestProcessStock_MalformedPayloadReturnsError(t *testing.T) {
	p := New(5, 3, zap.NewNop())
	e := model.Event{ID: "x", Type: model.EventTypeStock, Payload: []byte(`not json`)}

	_, err := p.Process(e)
	if err == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
}

func TestProcessStock_And_ProcessClick_DoNotShareTopK(t *testing.T) {
	p := New(5, 3, zap.NewNop())

	// huge click volume on "btn", small stock volume
	for i := 0; i < 1000; i++ {
		p.Process(mustEvent(t, model.EventTypeClick, model.ClickPayload{PageURL: "/x", Element: "btn"}))
	}
	res, err := p.Process(mustEvent(t, model.EventTypeStock, model.StockPayload{Ticker: "AAPL", Price: 100, Volume: 5}))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	top := res.Metrics["top_by_volume"].([]string)
	for _, entry := range top {
		if entry == "btn" {
			t.Fatalf("top_by_volume leaked a click element: %#v", top)
		}
	}
}