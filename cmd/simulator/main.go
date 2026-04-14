package main 

import(
	"os"
	"time"
	"context"
	"net/http"

	"github.com/xyn3x/stockflow/internal/simulator"
	"github.com/xyn3x/stockflow/pkg/config"
	"github.com/xyn3x/stockflow/pkg/logger"
	"github.com/xyn3x/stockflow/pkg/utils"
	"go.uber.org/zap"
)

func main() {
	log := logger.New("simulator")
	defer log.Sync()

	cfgPath := utils.EnvOrDefault("SIMULATOR_CONFIG_PATH", "configs/simulator.yaml")
	cfg, err := config.LoadSimulator(cfgPath)
	if err != nil {
		log.Fatal("Config is not loaded", zap.Error(err))
	}

	genConfig := &simulator.GeneratorConfig {
		StockTickers: 	cfg.Generator.StockTickers,
		ClickUsers:		cfg.Generator.ClickUsers,
		TelemetryApps 	cfg.Generator.TelemetryApps,
		StockRatio		cfg.Generator.StockRatio,
		ClickRatio		cfg.Generator.ClickRatio,
		TelemetryRatio	cfg.Generator.TelemetryRatio,
	}
	gen := simulator.NewGenerator(genConfig, log)

	interval := cfg.Generator.TickInterval
	if interval == 0 {
		interval = 100 * time.Millisecond
	}

	srv := server.NewServer(gen, interval, log)

	ctx, cancel := utils.WaitForShutdown()
	defer cancel()

	go srv.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := cfg.Server.Addr 
	if addr == "" {
		addr = ":8081"
	}

	httpSrv := &http.Server {
		Addr: 			addr, 
		Handler: 		mux, 
		ReadTimeout: 	10 * time.Second, 
		WriteTimeout:	10 * time.Second, 
	} 
	
	log.Info("Simulator is listening: ", zap.String("addr", addr), zap.Duration("tick", interval))

	go func() {
		<-ctx.Done()
		shutdownCtx, done := context.WithTimeout(context.Background(), 5 * time.Second)
		defer done()
		httpSrv.Shutdown(shutdownCtx)
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("HTTP server error", zap.Error(err))
		os.Exit(1)
	}

	log.Info("Simulator Shut Down Successfully.")
}	