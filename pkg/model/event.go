package model 

import(
	"encoding/json"
	"time"
)

type EventType string const(
	EventTypeStock		EventType = "stock"
	EventTypeClick 		EventType = "click"
	EventTypeTelemetry 	EventType = "telemetry"
)

type Event struct {
	ID			string			`json:"id"`
	TimeStamp	time.Time 		`json:"ts"`
	Source 		string 			`json:"source"` 	// simulator or binance-ws
	Type 		EventType		`json:"type"`
	Payload 	json.RawMessage	`json:"payload"`
}

type StockPayload struct {
	Ticker 		string		`json:"ticker"`
	Price		float64		`json:"price"`
	Bid			float64		`json:"bid"`
	Ask 		float64 	`json:"ask"`
	Volume		int64		`json:"volume"`
}

type ClickPayload struct {
	UsedID 		string		`json:"used_id"`
	SessionID 	string 		`json:"session_id"`
	PageURL 	string 		`json:"page_url"`
	Element 	string 		`json:"element"`		
}

type TelemetryPayload struct {
	ServiceName string            `json:"service_name"`
	MetricName  string            `json:"metric_name"`
	Value       float64           `json:"value"`
	Unit        string            `json:"unit"`
	Labels      map[string]string `json:"labels,omitempty"`
}

