package processing

import (
	"database/sql"
	"net/http"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"

	"griddog/internal/config"
	"griddog/internal/httpx"
)

// Server holds the processing-backend dependencies.
type Server struct {
	cfg  config.Config
	db   *sql.DB
	ch   *amqp.Channel
	mqtt mqtt.Client // flow 4: EMQX publish path (armed via SetMQTT after Connect)
}

// NewServer builds a processing-backend server. The MQTT client is attached later
// via SetMQTT (see cmd/processing/main.go) because emqx.Connect needs the server's
// OnMQTTConnect handler.
func NewServer(cfg config.Config, database *sql.DB, ch *amqp.Channel) *Server {
	return &Server{cfg: cfg, db: database, ch: ch}
}

// SetMQTT attaches the connected EMQX client so processing can publish replies.
func (s *Server) SetMQTT(client mqtt.Client) { s.mqtt = client }

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
