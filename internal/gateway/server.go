package gateway

import (
	"database/sql"
	"net/http"

	messaging "github.com/natthadechmani/go-rabbitmq-messaging"

	"griddog/internal/config"
	"griddog/internal/httpx"
)

// Server holds the gateway-backend dependencies.
type Server struct {
	cfg     config.Config
	db      *sql.DB
	mq      *messaging.Client // instrumented RabbitMQ client (shared library)
	pending *pendingRegistry
	client  *http.Client // no timeout: used for the long-lived SSE proxy
}

// NewServer builds a gateway-backend server.
func NewServer(cfg config.Config, database *sql.DB, mq *messaging.Client) *Server {
	return &Server{
		cfg:     cfg,
		db:      database,
		mq:      mq,
		pending: newPendingRegistry(),
		client:  &http.Client{},
	}
}

// Routes returns the HTTP handler for the gateway-backend (browser-facing).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/sse-call", s.handleSSECall)
	mux.HandleFunc("POST /api/rabbitmq-call", s.handleRabbitMQCall)
	mux.HandleFunc("POST /api/http-call", s.handleHTTPCall)
	mux.HandleFunc("GET /api/messages", s.handleMessages)
	return withCORS(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// withCORS adds permissive CORS headers so the React app can also call the
// gateway cross-origin during local development (the docker/nginx and vite-proxy
// setups are same-origin and don't strictly need it).
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
