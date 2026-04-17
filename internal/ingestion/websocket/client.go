package websocket 

import(
	"time"
	"context"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type MessageHandler func(data []byte)

type ClientConfig struct {
	URL 			string 
	ReconnectDelay 	time.Duration 
	MaxReconnects 	int 
	PingInterval 	time.Duration  
}

type Client struct {
	cfg 	ClientConfig
	log		*zap.Logger
}

func NewClient(cfg ClientConfig, log *zap.Logger) *Client {
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = 2 * time.Second
	}

	if cfg.PingInterval == 0 {
		cfg.PingInterval = 30 * time.Second 
	}

	return &Client {
		cfg:	cfg, 
		log: 	log, 
	}
}

func (c *Client) Run(ctx context.Context, handler MessageHandler) {
	attempt := 0
	delay := c.cfg.ReconnectDelay

	for {
		if c.cfg.MaxReconnects > 0 && attempt > c.cfg.MaxReconnects {
			c.log.Error("Client error: max reconnects reached", zap.Int("attempts", attempt), zap.String("url", c.cfg.URL))
			return 
		}

		if attempt > 0 {
			c.log.Info("Client reconnection", zap.Int("attempt", attempt), zap.Duration("delay", delay))

			select {
			case <-ctx.Done():
				return 
			case <-time.After(delay):
			}

			delay = min(delay * 2, 30 * time.Second)
		}

		attempt++ 
		err := c.connect(ctx, handler)
		if ctx.Err() != nil {
			return 
		}

		c.log.Warn("Client connection lost", zap.Int("attempt", attempt), zap.Error(err))
	}
}

func (c *Client) connect(ctx context.Context, handler MessageHandler) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, c.cfg.URL, nil)
	if err != nil {
		return err 
	}
	defer conn.Close()

	c.log.Info("Connected successfully", zap.String("url", c.cfg.URL))

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(c.cfg.PingInterval * 2))
		return nil
	})

	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(c.cfg.PingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return 
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()
	defer func() { <-pingDone }()

	go func() {
		<-ctx.Done()
		conn.WriteMessage(websocket.CloseMessage, 
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"))
		conn.Close()
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(c.cfg.PingInterval * 2))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil 
			}
			return err 
		}
		handler(msg)
	}
}