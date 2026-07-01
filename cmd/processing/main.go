package main

import (
	"context"
	"log"
	"net/http"

	messaging "github.com/natthadechmani/go-rabbitmq-messaging"

	"griddog/internal/config"
	"griddog/internal/db"
	"griddog/internal/processing"
	"griddog/internal/queues"
)

func main() {
	cfg := config.Load("8081")

	database, err := db.Connect(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer database.Close()
	if err := db.EnsureSchema(context.Background(), database); err != nil {
		log.Fatalf("schema: %v", err)
	}

	// Instrumented RabbitMQ client from the shared library (APM + DSM baked in).
	mq, err := messaging.New(cfg.RabbitMQURL, messaging.WithService("processing-backend"))
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer mq.Close()
	if err := mq.DeclareQueues(queues.Processing, queues.Completed); err != nil {
		log.Fatalf("declare queues: %v", err)
	}

	srv := processing.NewServer(cfg, database, mq)
	if err := srv.StartConsumer(context.Background()); err != nil {
		log.Fatalf("consumer: %v", err)
	}

	addr := ":" + cfg.Port
	log.Printf("processing-backend listening on %s", addr)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatalf("http: %v", err)
	}
}
