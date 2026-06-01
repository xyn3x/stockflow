package handler 

import(
	"encoding/json"
	"net/http"

	"github.com/xyn3x/stockflow/internal/api/store"
	apiws "github.com/xyn3x/stockflow/internal/api/websocket"
	"go.uber.org/zap"
)

type REST struct {
	log 	*zap.Logger 
	store 	*store.Store
	hub 	*apiws.Hub
}

func NewREST(st *store.Store, hub *apiws.Hub, log *zap.Logger) *REST {
	return &REST{log: log, store: st, hub: hub}
}

func (h *REST) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.health)
	mux.HandleFunc("/api/ticker/", h.getTicker)
	mux.HandleFunc("/api/top/", h.getTopK)
	mux.HandleFunc("/api/telemetry/", h.getTelemetry)
	mux.HandleFunc("/api/status", h.getStatus)
}

func (h *REST) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (h *REST) getTicker(w http.ResponseWriter, r *http.Request) {
	ticker := r.URL.Path[len("/api/ticker/"):]
	if ticker == "" {
		http.Error(w, "missing ticker name", http.StatusBadRequest)
		return 
	}

	snap, err := h.store.GetTicker(r.Context(), ticker)
	if err != nil {
		h.log.Error("get ticker", zap.String("ticker", ticker), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return 
	}
	if snap == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return 
	}
	writeJSON(w, snap)
}

func (h *REST) getTopK(w http.ResponseWriter, r *http.Request) {
	category := r.URL.Path[len("/api/top/"):]
	if category == "" {
		http.Error(w, "missing category", http.StatusBadRequest)
		return 
	}

	entries, err := h.store.GetTopK(r.Context(), category)
	if err != nil {
		h.log.Error("get category", zap.String("category", category), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return 
	}
	if entries == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return 
	}
	writeJSON(w, map[string] any{"category": category, "top": entries})
}

func (h *REST) getTelemetry(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/api/telemetry/"):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return 
	}

	snap, err := h.store.GetTelemetry(r.Context(), key) 
	if err != nil {
		h.log.Error("get telemetry", zap.String("key", key), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return 
	}
	if snap == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return 
	}
	writeJSON(w, snap)
}

func (h *REST) getStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string] any {
		"ws_client": 	h.hub.ClientCount(), 
		"status": 		"ok",
	})
}

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}