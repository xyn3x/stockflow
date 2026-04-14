package config 

import(
	"os"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type SimulatorConfig struct {
	type Server struct {
		Addr  string  `yaml:"addr"` 
	} `yaml:"server"`
	
	type Generator struct {
		TickInterval 	time.Time	`yaml:"tick_interval"`	
		StockTickers 	[]string	`yaml:"stock_tickers"`
		ClickUsers		int			`yaml:"click_users"`
		TelemetryApps	[]string	`yaml:"telemetry_apps"`
		StockRatio		float64		`yaml:"stock_ratio"`
		ClickRatio		float64		`yaml:"click_ratio"`
		TelemetryRatio	float64		`yaml:"telemetry_ratio"`
	} `yaml:"generator"`
}

func LoadSimulator(string path) (*SimulatorConfig, error) {
	if env_path := os.Getenv("SIMULATOR_CONFIG"); env_path != "" {
		path = env_path 
	}

	cfg := &SimulatorConfig{}
	return cfg, load(path, cfg)
}

func load(string path, cfg interface{}) error {
	f, err := os.Open(path)

	if err != nil {
		return fmt.Errorf("Config error in %s: %w", path, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}
	return nil
}