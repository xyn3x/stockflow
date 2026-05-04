package main 

import(
	"os"
	"time"

	ingestws "github.com/xyn3x/stockflow/internal/ingestion/websocket"
	"github.com/xyn3x/stockflow/internal/ingestion/parser"
	"github.com/xyn3x/stockflow/internal/ingestion/publisher"
	"github.com/xyn3x/stockflow/pkg/config"
	"github.com/xyn3x/stockflow/pkg/logger"
	"github.com/xyn3x/stockflow/pkg/utils"
	"go.uber.org/zap"
)

func main() {
	log := logger.New("ingestion")
	defer log.Sync()

	cfgPath := utils.EnvOrDefault("INGESTION_CONFIG", "configs/ingestion.yaml")
	cfg, err := config.LoadIngestion(cfgPath)
	if err != nil {
		log.Fatal("Config is not loaded", zap.Error(err))
	}

	prc := parser.New(0)

	streamName := cfg.NATS.StreamName 
	if streamName == "" {
		streamName = "EVENTS"
	}

	streamSubjects := cfg.NATS.StreamSubjects
	if len(streamSubjects) == 0 {
		streamSubjects = append(streamSubjects, "events.raw.>")
	}

	pubCfg := publisher.Config {
		URL: 			cfg.NATS.URL, 
		ConnectTimeout: cfg.NATS.ConnectTimeout, 
		MaxReconnects: 	cfg.NATS.MaxReconnects, 
		StreamName: 	streamName, 
		StreamSubjects: streamSubjects, 
		StreamMaxAge:	cfg.NATS.StreamMaxAge, 
		StreamMaxBytes:	cfg.NATS.StreamMaxBytes,
		BatchSize:	 	cfg.Publisher.BatchSize, 
		FlushTimeout: 	cfg.Publisher.FlushTimeout,
	}
	pub, err := publisher.New(pubCfg, log)
	if err != nil {
		log.Fatal("Publisher is not connected", zap.Error(err))
	}
	defer pub.Close()

	wsCfg := ingestws.ClientConfig {
		URL : 				cfg.Source.WebSocketURL, 
		ReconnectDelay : 	cfg.Source.ReconnectDelay, 
		MaxReconnects : 	cfg.Source.MaxReconnects, 
		PingInterval : 		cfg.Source.PingInterval,
	}
	wsClient := ingestws.NewClient(wsCfg, log)


	ctx, cancel := utils.WaitForShutdown()
	defer cancel()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				published, dropped := pub.Stats()
				log.Info("Publisher stats:", zap.Uint64("published", published), zap.Uint64("dropped", dropped))
			}
		}
	}()

	go pub.Run(ctx)

	log.Info("Ingestion service is starting", zap.String("source", cfg.Source.WebSocketURL), zap.String("nats", cfg.NATS.URL))

	handler := func(data []byte) {
		msg, err := prc.Parse(data)
		if err != nil {
			log.Debug("Parser error", zap.Error(err), zap.Int("bytes", len(data)))
			return 
		}
		log.Info("Event is parsed", 
			zap.String("id", msg.Event.ID), 
			zap.String("event", string(msg.Event.Type)), 
			zap.Duration("latency", msg.ParseLatency))
		pub.Publish(msg.Event)
	}

	wsClient.Run(ctx, handler)

	published, dropped := pub.Stats()
	log.Info("Ingestion service shut down", zap.Uint64("published", published), zap.Uint64("dropped", dropped))
	os.Exit(0)
}