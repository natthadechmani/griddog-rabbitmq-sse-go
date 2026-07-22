package gateway

import (
	"database/sql"
	"net/http"

	"github.com/eclipse/paho.golang/autopaho"
	amqp "github.com/rabbitmq/amqp091-go"

	"griddog/internal/config"
	"griddog/internal/emqx"
	"griddog/internal/httpx"
)

// Server holds the gateway-backend dependencies.
type Server struct {
	cfg         config.Config
	db          *sql.DB
	ch          *amqp.Channel
	pending     *pendingRegistry
	mqtt        *autopaho.ConnectionManager // flow 4: EMQX publish path (armed via SetMQTT after Connect)
	mqttPending *pendingRegistry            // flow 4: correlation waiters for completed-topic replies
	client      *http.Client     // no timeout: used for the long-lived SSE proxy
}

// NewServer builds a gateway-backend server. The MQTT client is attached later via
// SetMQTT (see cmd/gateway/main.go) because emqx.Connect needs the server's
// OnMQTTConnect handler, creating a chicken-and-egg with the client field.
func NewServer(cfg config.Config, database *sql.DB, ch *amqp.Channel) *Server {
	return &Server{
		cfg:         cfg,
		db:          database,
		ch:          ch,
		pending:     newPendingRegistry(),
		mqttPending: newPendingRegistry(),
		client:      &http.Client{},
	}
}

// SetMQTT attaches the connected EMQX connection manager so the request path can publish.
func (s *Server) SetMQTT(cm *autopaho.ConnectionManager) { s.mqtt = cm }

// MQTTHandlers bundles the callbacks autopaho needs at construction time: the
// resubscribe hook (OnConnectionUp) and the single inbound-message router (OnMessage).
func (s *Server) MQTTHandlers() emqx.Handlers {
	return emqx.Handlers{
		OnConnectionUp: s.OnMQTTConnect,
		OnMessage:      s.handleMQTTCompleted,
	}
}

// Routes returns the HTTP handler for the gateway-backend (browser-facing).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/sse-call", s.handleSSECall)
	mux.HandleFunc("POST /api/rabbitmq-call", s.handleRabbitMQCall)
	mux.HandleFunc("POST /api/mqtt-call", s.handleMQTTCall)
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
