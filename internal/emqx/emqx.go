// Package emqx is a thin MQTT 5.0 (EMQX) client for the flow-4 round-trip, built on
// eclipse/paho.golang's autopaho connection manager (automatic reconnect + resubscribe).
//
// As in the previous MQTT 3.1.1 version, it deliberately carries NO Datadog tracing:
// no manual spans, no DSM checkpoints. The gateway's HTTP handler still gets an
// auto-instrumented net/http span (Orchestrion); the MQTT hops themselves are
// intentionally uninstrumented at this phase. Moving to MQTT 5.0 is the prerequisite
// for EMQX-native OpenTelemetry tracing (which propagates W3C traceparent via MQTT 5.0
// User Properties) — a later phase.
package emqx

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// Topics shared by both services. MQTT-style hierarchy, distinct from the RabbitMQ
// queue names.
const (
	RequestTopic   = "griddog/mqtt/requests"  // topic A: gateway -> processing
	CompletedTopic = "griddog/mqtt/completed" // topic B: processing -> gateway

	// QoS 1 (at-least-once) on both publish and subscribe. paho.Publish.QoS and
	// paho.SubscribeOptions.QoS are both byte, so this const is reused unchanged.
	QoS = byte(1)
)

// Trace carries W3C trace context across the MQTT hop as MQTT 5.0 User Properties
// (traceparent/tracestate). It is propagation only — EMQX authors the broker spans;
// the app just passes the context through so those spans join the caller's trace.
// The zero value means "nothing to propagate".
type Trace struct {
	Traceparent string
	Tracestate  string
}

func (t Trace) empty() bool { return t.Traceparent == "" }

// pubProperties renders the trace context as MQTT 5.0 publish User Properties, or nil.
//
// NOTE: we deliberately send ONLY `traceparent`, not `tracestate`. EMQX 6.2.2 fails to
// parse dd-trace-go's tracestate value (`dd=s:1;p:...;t.dm:-1;t.tid:...`, which contains
// ':' and ';' inside the value) and, on that parse failure, discards the ENTIRE inbound
// trace context — so the message isn't traced and EMQX won't adopt our trace id. The
// traceparent alone carries the trace id, parent span id, and sampled flag, which is all
// EMQX needs to continue (adopt) the trace. (Verified: traceparent-only is adopted;
// traceparent+dd-tracestate is not.)
func (t Trace) pubProperties() *paho.PublishProperties {
	if t.empty() {
		return nil
	}
	up := paho.UserProperties{}
	up.Add("traceparent", t.Traceparent)
	return &paho.PublishProperties{User: up}
}

// traceFromPacket reads the W3C trace context from an inbound PUBLISH's User Properties.
func traceFromPacket(p *paho.Publish) Trace {
	if p == nil || p.Properties == nil {
		return Trace{}
	}
	return Trace{
		Traceparent: p.Properties.User.Get("traceparent"),
		Tracestate:  p.Properties.User.Get("tracestate"),
	}
}

// Handlers bundles the two server-supplied callbacks that autopaho needs at
// construction time (OnConnectionUp and the inbound-PUBLISH router are wired into the
// ClientConfig before the ConnectionManager exists).
type Handlers struct {
	// OnConnectionUp runs on every (re)connection with the live ConnectionManager so
	// the caller can (re)subscribe. Replaces the v3 mqtt.OnConnectHandler.
	OnConnectionUp func(cm *autopaho.ConnectionManager)
	// OnMessage is the single inbound-PUBLISH router. payload is owned by paho; copy it
	// if it is retained past the call (both callers do). tr carries any propagated W3C
	// trace context from the message's User Properties.
	OnMessage func(topic string, payload []byte, tr Trace)
}

// Connect dials EMQX over MQTT 5.0 and returns a connected ConnectionManager. autopaho
// owns reconnection; a bounded AwaitConnection replaces the old 30x2s loop so startup
// still fails fast if the broker never comes up. Pass a long-lived ctx (the manager's
// lifetime is tied to it) — e.g. context.Background().
func Connect(ctx context.Context, brokerURL, clientID string, h Handlers) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(brokerURL) // "tcp://emqx:1883" — scheme "tcp" is accepted by autopaho
	if err != nil {
		return nil, fmt.Errorf("parse broker url %q: %w", brokerURL, err)
	}

	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     30,               // seconds; == v3 SetKeepAlive(30s)
		ConnectTimeout:                10 * time.Second, // == v3 SetConnectTimeout(10s)
		CleanStartOnInitialConnection: true,             // with SessionExpiryInterval 0 == v3 SetCleanSession(true)
		SessionExpiryInterval:         0,                // broker drops the session (and subs) on disconnect
		ReconnectBackoff:              autopaho.NewConstantBackoff(2 * time.Second),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Printf("connected to EMQX as %s", clientID)
			h.OnConnectionUp(cm) // (re)subscribe on every (re)connection
		},
		OnConnectionDown: func() bool {
			log.Printf("emqx connection lost (%s), reconnecting", clientID)
			return true // keep reconnecting; == v3 SetAutoReconnect(true)
		},
		OnConnectError: func(err error) {
			log.Printf("emqx connect error (%s): %v", clientID, err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnClientError: func(err error) {
				log.Printf("emqx client error (%s): %v", clientID, err)
			},
			OnServerDisconnect: func(_ *paho.Disconnect) {
				log.Printf("emqx server-initiated disconnect (%s)", clientID)
			},
			// Single router for all inbound PUBLISH packets. With
			// EnableManualAcknowledgment left false, QoS 1 is auto-acked after this
			// returns — same fire-and-forget semantics as the v3 client.
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					h.OnMessage(pr.Packet.Topic, pr.Packet.Payload, traceFromPacket(pr.Packet))
					return true, nil
				},
			},
			// Session left nil => in-memory state, consistent with clean-session.
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("emqx new connection (%s): %w", clientID, err)
	}

	// Bounded wait for the first successful connection (derived ctx so cancelling it
	// does not tear down the manager).
	awaitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := cm.AwaitConnection(awaitCtx); err != nil {
		_ = cm.Disconnect(context.Background())
		return nil, fmt.Errorf("could not connect to EMQX (%s): %w", clientID, err)
	}
	return cm, nil
}

// Publish sends body to topic at QoS 1, retain=false, bounded to ~5s. autopaho's
// Publish does not itself wait for connectivity, so AwaitConnection first preserves the
// old WaitTimeout(5s) "wait for the broker" behaviour instead of failing instantly
// during a reconnect.
func Publish(ctx context.Context, cm *autopaho.ConnectionManager, topic string, body []byte, tr Trace) error {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cm.AwaitConnection(pctx); err != nil {
		return fmt.Errorf("mqtt publish %s: connection down: %w", topic, err)
	}
	if _, err := cm.Publish(pctx, &paho.Publish{
		Topic:      topic,
		QoS:        QoS,
		Retain:     false,
		Payload:    body,
		Properties: tr.pubProperties(), // carries W3C traceparent as MQTT 5.0 User Property (or nil)
	}); err != nil {
		return fmt.Errorf("mqtt publish %s: %w", topic, err)
	}
	return nil
}
