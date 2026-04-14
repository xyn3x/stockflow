package simulator 

import(
	"encoding/json"
	"time"
	"sync"
	"net/http"
	"context"
	
	"github.com/xyn3x/stockflow/pkg/model"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader {
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize: 1024,
	WriteBufferSize: 4096,
} 

type client struct {
	conn 	*websocket.Conn 
	send 	chan []byte 
	closed 	chan struct{}
}

type Server struct {
	log 		*zap.Logger 
	mu 			*sync.RWMutex 
	gen 		*Generator 
	interval 	time.Duration 

	clients 	map[*client] struct {}
}

func NewServer (gen *Generator, interval time.Duration, log *zap.Logger) *Server {
	return &Server {
		log: log, 
		gen: gen, 
		interval: interval, 
		clients: make(map[*client] struct{}),
	}
}

func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.log.Info("Broadcast loop started: ", zap.Duration("Interval", s.interval))

	for {
		select {
		case <-ctx.Done():
			s.log.Info("Broadcast loop disconnected.")
			s.disconnectAll()
			return 
		case <-ticker.C:
			event := s.gen.Next()
			s.broadcast(event)
		}
	}
}

func (s *Server) broadcast(event model.Event) {
	msg, err := json.Marshal(event)

	if err != nil {
		s.log.Error("JSON Marshal error occured", zap.Error(err))
		return 
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for cur := range s.clients {
		select {
		case cur.send <-msg: 
		default: 
			s.log.Warn("Slow Client, dropping frame", zap.String("remote", cur.conn.RemoteAddr().String()))
		}
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		s.log.Error("Websocket upgrader error occured", zap.Error(err))
		return 
	}

	c := &client {
		conn: conn, 
		send: make(chan[] byte, 256),
		closed: make(chan struct{}),
	}

	s.register(c)
	s.log.Info("Client connected", zap.String("remote", c.conn.RemoteAddr().String()))

	go s.writePump(c)
	s.readPump(c)
}

func (s *Server) writePump(c *client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			return 
		case msg, ok := <-c.send: 
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, nil)
				return 
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				s.log.Debug("Write Error", zap.Error(err))
				return 
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return 
			}
		}
	}
}

func (s *Server) readPump(c *client) {
	defer func() {
		s.unregister(c)
		close(c.closed)
		c.conn.Close()
		s.log.Info("Client Disconnected", zap.String("remote", c.conn.RemoteAddr().String()))
	}()

	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil 
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				s.log.Debug("Unexpected Close Error", zap.Error(err))
			}
			return 
		}
	}
}

func (s *Server) register(c *client) {
	s.mu.Lock()
	s.clients[c] = struct{}{} 
	s.mu.Unlock()
}

func (s *Server) unregister(c *client) {
	s.mu.Lock()
	delete(s.clients, c) 
	s.mu.Unlock()
}

func (s *Server) disconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	for cur := range s.clients {
		close(cur.send)
	}
}