package processing

import (
	"database/sql"
	"net/http"

	messaging "github.com/natthadechmani/go-rabbitmq-messaging"

	"griddog/internal/config"
	"griddog/internal/httpx"
)

// Server holds the processing-backend dependencies.
type Server struct {
	cfg config.Config
	db  *sql.DB
	mq  *messaging.Client // instrumented RabbitMQ client (shared library)
}

// NewServer builds a processing-backend server.
func NewServer(cfg config.Config, database *sql.DB, mq *messaging.Client) *Server {
	return &Server{cfg: cfg, db: database, mq: mq}
}

// Routes returns the HTTP handler for the processing-backend.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /sse-stream", s.handleSSEStream)
	mux.HandleFunc("POST /process", s.handleProcess)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
