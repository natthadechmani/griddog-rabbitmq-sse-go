package main

import (
	"context"
	"log"
	"net/http"

	"griddog/internal/config"
	"griddog/internal/db"
	"griddog/internal/gateway"
	"griddog/internal/rabbitmq"
)

func main() {
	cfg := config.Load("8080")

	database, err := db.Connect(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer database.Close()
	if err := db.EnsureSchema(context.Background(), database); err != nil {
		log.Fatalf("schema: %v", err)
	}

	conn, ch, err := rabbitmq.Connect(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer conn.Close()
	defer ch.Close()
	if err := rabbitmq.DeclareQueues(ch, rabbitmq.ProcessingQueue, rabbitmq.CompletedQueue); err != nil {
		log.Fatalf("declare queues: %v", err)
	}

	srv := gateway.NewServer(cfg, database, ch)
	if err := srv.StartCompletedConsumer(context.Background()); err != nil {
		log.Fatalf("completed consumer: %v", err)
	}

	addr := ":" + cfg.Port
	log.Printf("gateway-backend listening on %s", addr)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatalf("http: %v", err)
	}
}
