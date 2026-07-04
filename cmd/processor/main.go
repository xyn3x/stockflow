package main 

import(
	"os"
	"net/http"

	"github.com/xyn3x/stockflow/internal/processor/pipeline"
	"github.com/xyn3x/stockflow/internal/processor/worker"
	"github.com/xyn3x/stockflow/pkg/config"
	"github.com/xyn3x/stockflow/pkg/logger"
	"github.com/xyn3x/stockflow/pkg/utils"
	"github.com/xyn3x/stockflow/pkg/metrics"
	"go.uber.org/zap"
)

func main() {
	log := logger.New("processor")
	defer log.Sync()
	
	cfgPath := utils.EnvOrDefault("PROCESSOR_CONFIG", "configs/processor.yaml")
	cfg, err := config.LoadProcessor(cfgPath)
	if err != nil {
		log.Fatal("error: config is not loaded", zap.Error(err))
	}

	m := metrics.New("processor")
	
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		http.ListenAndServe(":9092", mux)
	}()

	pl := pipeline.New(
		cfg.Pipeline.MovingAvgWindow, 
		cfg.Pipeline.TopK, 
		log,
	)

	wrCfg := worker.Config {
		NATSURL: 		cfg.NATS.URL,
		ConnectTimeout: cfg.NATS.ConnectTimeout, 
		StreamName: 	cfg.NATS.StreamName, 
		ConsumerName:	cfg.NATS.ConsumerName, 
		FilterSubject:	cfg.NATS.FilterSubject,
		FetchBatch:		cfg.Worker.FetchBatch,
		FetchTimeout:	cfg.Worker.FetchTimeout,
	}
	wr, err := worker.New(wrCfg, pl, log, m)
	if err != nil {
		log.Fatal("error: worker initialization", zap.Error(err))
	}
	defer wr.Close()

	ctx, cancel := utils.WaitForShutdown()
	defer cancel()

	log.Info("processor service is starting", 
		zap.String("nats", cfg.NATS.URL), 
		zap.String("stream", cfg.NATS.StreamName),
		zap.String("filter", cfg.NATS.FilterSubject))
	
	if err := wr.Run(ctx); err != nil {
		log.Error("worker error", zap.Error(err))
		os.Exit(1)
	}
	

	log.Info("processor shut down successfully")
}