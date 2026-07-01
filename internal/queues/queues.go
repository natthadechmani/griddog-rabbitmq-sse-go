// Package queues holds the queue names shared by the gateway and processing services.
// (Queue names are app-specific, so they live here rather than in the shared messaging
// library.)
package queues

const (
	Processing = "processing-queue" // gateway -> processing
	Completed  = "completed-queue"  // processing -> gateway
)
