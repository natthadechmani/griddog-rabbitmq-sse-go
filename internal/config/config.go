package config

import "os"

// Config holds runtime configuration sourced from environment variables.
type Config struct {
	Port              string
	MySQLDSN          string
	RabbitMQURL       string
	ProcessingBaseURL string
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment, falling back to sensible
// defaults that work when running the binaries directly on the host (with the
// docker-compose infra exposed on localhost).
func Load(defaultPort string) Config {
	return Config{
		Port:              getenv("PORT", defaultPort),
		MySQLDSN:          getenv("MYSQL_DSN", "root:rootpw@tcp(127.0.0.1:3306)/appdb?parseTime=true&multiStatements=true"),
		RabbitMQURL:       getenv("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/"),
		ProcessingBaseURL: getenv("PROCESSING_BASE_URL", "http://127.0.0.1:8081"),
	}
}
