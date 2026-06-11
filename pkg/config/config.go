package config 

import(
	"os"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type SimulatorConfig struct {
	Server struct {
		Addr  string  `yaml:"addr"` 
	} `yaml:"server"`
	
	Generator struct {
		TickInterval 	time.Duration	`yaml:"tick_interval"`	
		StockTickers 	[]string		`yaml:"stock_tickers"`
		ClickUsers		int				`yaml:"click_users"`
		TelemetryApps	[]string		`yaml:"telemetry_apps"`
		StockRatio		float64			`yaml:"stock_ratio"`
		ClickRatio		float64			`yaml:"click_ratio"`
		TelemetryRatio	float64			`yaml:"telemetry_ratio"`
	} `yaml:"generator"`
}

func LoadSimulator(path string) (*SimulatorConfig, error) {
	if env_path := os.Getenv("SIMULATOR_CONFIG"); env_path != "" {
		path = env_path 
	}

	cfg := &SimulatorConfig{}
	return cfg, load(path, cfg)
}

type IngestionConfig struct {
	Source struct {
		WebSocketURL 	string 			`yaml:"websocket_url"`
		ReconnectDelay 	time.Duration 	`yaml:"reconnect_delay"`
		MaxReconnects	int 			`yaml:"max_reconnects"`
		PingInterval 	time.Duration 	`yaml:"ping_interval"`
	} `yaml:"source"`

	NATS struct {	
		URL				string 			`yaml:"url"`
		ConnectTimeout	time.Duration 	`yaml:"connect_timeout"`
		MaxReconnects	int				`yaml:"max_reconnects"`
		StreamName		string			`yaml:"stream_name"`
		StreamSubjects	[]string		`yaml:"stream_subjects"`
		StreamMaxAge	time.Duration	`yaml:"stream_max_age"`
		StreamMaxBytes	int64			`yaml:"stream_max_bytes"`
	} `yaml:"nats"`

	Publisher struct {
		BatchSize		int				`yaml:"batch_size"`
		FlushTimeout	time.Duration	`yaml:"flush_timeout"`
	} `yaml:"publisher"`
}

func LoadIngestion(path string)	(*IngestionConfig, error) {
	if env_path := os.Getenv("INGESTION_CONFIG"); env_path != "" {
		path = env_path 
	}
	cfg := &IngestionConfig{}
	return cfg, load(path, cfg)
} 

type ProcessorConfig struct {
	NATS struct {
		URL 			string 			`yaml:"url"`
		ConnectTimeout	time.Duration 	`yaml:"connect_timeout"`
		StreamName 		string 			`yaml:"stream_name"`
		ConsumerName 	string 			`yaml:"consumer_name"`
		FilterSubject 	string 			`yaml:"filter_subject"`
	} `yaml:"nats"`

	Pipeline struct {
		MovingAvgWindow int 	`yaml:"moving_avg_window"`
		TopK			int 	`yaml:"top_k"`
	} `yaml:"pipeline"`

	Worker struct {
		FetchBatch		int 			`yaml:"fetch_batch"`
		FetchTimeout	time.Duration 	`yaml:"fetch_timeout"`
	} `yaml:"worker"`
}

func LoadProcessor(path string) (*ProcessorConfig, error) {
	if env_path := os.Getenv("PROCESSOR_CONFIG"); env_path != "" {
		path = env_path 
	}
	cfg := &ProcessorConfig{}
	return cfg, load(path, cfg)
}

type APIConfig struct {
	Server struct {
		Addr string `yaml:"addr"`
	} `yaml:"server"`
	
	NATS struct {
		URL 			string			`yaml:"url"`
		ConnectTimeout	time.Duration 	`yaml:"connect_timeout"`
		StreamName		string 			`yaml:"stream_name"`
		ConsumerName	string 			`yaml:"consumer_name"`
	} `yaml:"nats"`

	Redis struct {
		Addr 		string 	`yaml:"addr"`
		Password	string 	`yaml:"password"`
		DB			int 	`yaml:"db"`
	} `yaml:"redis"`
}

func LoadAPI(path string) (*APIConfig, error) {
	if env_path := os.Getenv("API_CONFIG"); env_path != "" {
		path = env_path 
	}
	cfg := &APIConfig{}
	return cfg, load(path, cfg)
}

func load(path string, cfg interface{}) error {
	f, err := os.Open(path)

	if err != nil {
		return fmt.Errorf("Config error in %s: %w", path, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}
	return nil
}