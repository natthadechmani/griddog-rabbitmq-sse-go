// Package emqx is a thin MQTT (EMQX) client for the flow-4 round-trip. It mirrors
// the structure of internal/rabbitmq but deliberately carries NO Datadog tracing:
// there are no manual spans and no DSM checkpoints here. The gateway's HTTP handler
// still gets an auto-instrumented net/http span (Orchestrion), but the MQTT hops
// themselves are intentionally uninstrumented for this phase.
package emqx

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Topics shared by both services. Named with an MQTT-style hierarchy so they are
// clearly distinct from the RabbitMQ queue names (processing-queue/completed-queue).
const (
	RequestTopic   = "griddog/mqtt/requests"  // topic A: gateway -> processing
	CompletedTopic = "griddog/mqtt/completed" // topic B: processing -> gateway

	// QoS 1 (at-least-once) on both publish and subscribe. Delivered QoS is
	// min(pub, sub), so both sides must be >= 1 for reliable round-trips.
	QoS = byte(1)
)

// Connect dials EMQX and returns a connected client, retrying like rabbitmq.Connect
// (30 attempts, 2s apart). onConnect is wired as the OnConnectHandler so that
// subscriptions are (re)established on every (re)connection — with CleanSession the
// broker drops subscription state on disconnect, so re-subscribing here is required.
//
// onConnect receives the client from paho, so it can Subscribe safely even before the
// caller has stored the returned client (see the main.go wiring order).
func Connect(url, clientID string, onConnect mqtt.OnConnectHandler) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(url).
		SetClientID(clientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetOnConnectHandler(onConnect).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("emqx connection lost (%s): %v", clientID, err)
		}).
		SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
			log.Printf("emqx reconnecting (%s)", clientID)
		}).
		// Catch-all so an unexpected message on an unsubscribed topic (e.g. a typo)
		// is visible rather than silently dropped.
		SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
			log.Printf("emqx UNEXPECTED message on %s (%d bytes)", m.Topic(), len(m.Payload()))
		})
	// NOTE: intentionally NOT SetConnectRetry(true) — that makes Connect() never
	// return while the broker is down, which would hide the bounded startup loop below.

	client := mqtt.NewClient(opts)
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		tok := client.Connect()
		if tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
			log.Printf("connected to EMQX as %s", clientID)
			return client, nil
		}
		lastErr = tok.Error()
		log.Printf("waiting for EMQX (attempt %d/30): %v", attempt, lastErr)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("could not connect to EMQX after retries: %w", lastErr)
}

// Publish sends body to topic at QoS 1, retained=false, surfacing token errors. A
// short WaitTimeout keeps callers (the HTTP request path) from hanging on a stuck
// broker. retained MUST be false: a retained request would be reprocessed on every
// resubscribe, and a retained reply would be pushed to any fresh gateway subscriber.
func Publish(client mqtt.Client, topic string, body []byte) error {
	tok := client.Publish(topic, QoS, false, body)
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return fmt.Errorf("mqtt publish %s: %w", topic, tok.Error())
	}
	return nil
}
