package websocket

import(
	"encoding/json"
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xyn3x/stockflow/pkg/metrics"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader {
	CheckOrigin: 		func(r *http.Request) bool { return true },
	ReadBufferSize: 	1024, 
	WriteBufferSize: 	4096, 
}

type client struct {
	conn 	*websocket.Conn
	send 	chan []byte
	closed	chan struct{}
}

type Hub struct {
	log 	*zap.Logger 
	mu 		sync.RWMutex 
	m 		*metrics.Metrics
	clients map[*client] struct{}
}

func NewHub(log *zap.Logger, m *metrics.Metrics) *Hub {
	return &Hub {
		log: log, 
		m:	 m,
		clients: make(map[*client] struct{}),
	}
}

func (h *Hub) Broadcast(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		h.log.Error("json broadcast marshal error", zap.Error(err))
		return 
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for cl := range h.clients {
		select {
		case cl.send <- data:
		default:
			h.log.Warn("WS client is slow, dropping the frame", zap.String("remote", cl.conn.RemoteAddr().String()))
		}
	}
}


func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("WS upgrade failed", zap.Error(err))
		return 
	}

	cl := &client {
		conn: 	conn, 
		send:	make(chan []byte, 256),
		closed: make(chan struct{}),
	}

	h.register(cl)
	h.log.Info("WS client connected", zap.String("remote", cl.conn.RemoteAddr().String()))

	go h.writePump(cl)
	h.readPump(cl)
}

func (h *Hub) writePump(cl *client) {
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-cl.closed:
			return 
		case msg, ok := <-cl.send:
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				cl.conn.WriteMessage(websocket.CloseMessage, nil)
				return 
			}
			if err := cl.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return 
			}
		case <-ping.C:
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := cl.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return 
			}
		}
	}
}

func (h *Hub) readPump(cl *client) {
	defer func() {
		h.unregister(cl)
		close(cl.closed)
		cl.conn.Close()
		h.log.Info("WS client disconnected", zap.String("remote", cl.conn.RemoteAddr().String()))
	}()

	cl.conn.SetReadLimit(512)
	cl.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	cl.conn.SetPongHandler(func (string) error {
		cl.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil 
	})

	for {
		if _, _, err := cl.conn.ReadMessage(); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				h.log.Debug("WS unexpected closed", zap.Error(err))
			}
			return 
		}
	}
}

func (h *Hub) register(cl *client) {
	h.mu.Lock()
	h.clients[cl] = struct{}{}
	h.m.WSClients.Inc()
	h.mu.Unlock()
}

func (h *Hub) unregister(cl *client) {
	h.mu.Lock()
	delete(h.clients, cl)
	h.m.WSClients.Dec()
	h.mu.Unlock()
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) Run(ctx context.Context) {
	<-ctx.Done()
	h.mu.Lock()
	defer h.mu.Unlock()
	for cl := range h.clients {
		close(cl.send)
	}
}