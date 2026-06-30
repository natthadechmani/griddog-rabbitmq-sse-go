package processing

import (
	"database/sql"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"

	"griddog/internal/config"
	"griddog/internal/httpx"
)

// Server holds the processing-backend dependencies.
type Server struct {
	cfg config.Config
	db  *sql.DB
	ch  *amqp.Channel
}

// NewServer builds a processing-backend server.
func NewServer(cfg config.Config, database *sql.DB, ch *amqp.Channel) *Server {
	return &Server{cfg: cfg, db: database, ch: ch}
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
