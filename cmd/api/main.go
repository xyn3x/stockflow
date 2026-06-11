package main 

import(
	"context"
	"net/http"
	"os"
	"time"


	"github.com/xyn3x/stockflow/internal/api/handler"
	"github.com/xyn3x/stockflow/internal/api/store"
	apiws "github.com/xyn3x/stockflow/internal/api/websocket"
	"github.com/xyn3x/stockflow/pkg/config"
	"github.com/xyn3x/stockflow/pkg/logger"
	"github.com/xyn3x/stockflow/pkg/utils"
	"go.uber.org/zap"
)

func main() {
	log := logger.New("api")
	defer log.Sync()

	cfgPath := utils.EnvOrDefault("API_CONFIG", "configs/api.yaml")
	cfg, err := config.LoadAPI(cfgPath)
	if err != nil {
		log.Fatal("load config", zap.Error(err))
	}

	st := store.New(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	ctx, cancel := utils.WaitForShutdown()
	defer cancel()

	if err := st.Ping(ctx); err != nil {
		log.Fatal("redis ping", zap.Error(err))
	}
	defer st.Close()
	log.Info("reddis connected", zap.String("addr", cfg.Redis.Addr))

	hub := apiws.NewHub(log)
	go hub.Run(ctx)

	sub, err := handler.NewSubscriber(
		cfg.NATS.URL, 
		cfg.NATS.ConnectTimeout, 
		cfg.NATS.StreamName,
		cfg.NATS.ConsumerName, 
		hub, 
		st, 
		log, 
	)
	if err != nil {
		log.Fatal("subscriber init", zap.Error(err))
	}
	defer sub.Close()
	go func() {
		if err := sub.Run(ctx); err != nil {
			log.Error("subscriber error", zap.Error(err))
		}
	}()

	rest := handler.NewREST(st, hub, log)
	mux := http.NewServeMux()
	rest.RegisterRoutes(mux)
	mux.Handle("/ws", hub)

	addr := cfg.Server.Addr 
	if addr == "" {
		addr = ":8080"
	}

	srv := &http.Server {
		Addr: 			addr, 
		Handler: 		mux, 
		ReadTimeout: 	10 * time.Second, 
		WriteTimeout: 	10 * time.Second, 
	}

	go func() {
		<-ctx.Done()
		shutCtx, done := context.WithTimeout(context.Background(), 5 * time.Second)
		defer done()
		srv.Shutdown(shutCtx)
	}()

	log.Info("api service started", zap.String("addr", addr), 
									zap.String("nats", cfg.NATS.URL), 
									zap.String("redis", cfg.Redis.Addr))
	
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("http server", zap.Error(err))
		os.Exit(1)
	}

	log.Info("api shut down successfully")
}