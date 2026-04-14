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