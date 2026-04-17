package parser 


import(
	"time"
	"fmt"
	"encoding/json"

	"github.com/xyn3x/stockflow/pkg/model"
)

type ParsedEvent struct {
	Event 			model.Event 
	RecievedAt 		time.Time 
	ParseLatency 	time.Duration 
}

type Parser struct {
	maxPayloadBytes	int
}


func New(maxPayloadBytes int) *Parser {
	if maxPayloadBytes == 0 {
		maxPayloadBytes = 1 << 20 
	}

	return &Parser {
		maxPayloadBytes: maxPayloadBytes,
	}
}

func (p *Parser) Parse(data []byte) (*ParsedEvent, error) {
	start := time.Now()

	if len(data) > p.maxPayloadBytes {
		return nil, fmt.Errorf("Parser error: payload is too large: %d bytes (max %d bytes)", len(data), p.maxPayloadBytes)
	} 

	var event model.Event 
	if err := json.Unmarshal(data, &event); err != nil  {
		return nil, fmt.Errorf("Parser error: json unmarshall error -> %w", err)
	}

	if err := validate(&event); err != nil {
		return nil, fmt.Errorf("Parser error: Invalid event -> %w", err)
	}

	return &ParsedEvent {
		Event: event, 
		RecievedAt: start, 
		ParseLatency: time.Since(start),
	}, nil 
}

func validate(e *model.Event) error {
	if e.ID == "" {
		return fmt.Errorf("Event error: Missing Id")
	}
	if e.TimeStamp.IsZero() {
		return fmt.Errorf("Event error: Missing or Wrong Timestamp")
	}
	if e.Source == "" {
		return fmt.Errorf("Event error: Missing source")
	}
	switch e.Type {
	case model.EventTypeClick, model.EventTypeStock, model.EventTypeTelemetry:
	default:
		return fmt.Errorf("Event error: Invalid type %q", e.Type)
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("Event error: Empty payload")
	}
	return nil
}