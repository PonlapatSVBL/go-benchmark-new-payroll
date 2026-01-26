package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/joho/godotenv"
)

/*
========================
 Utilities
========================
*/

func stringValue(ptr *string, fallback string) string {
	if ptr == nil || *ptr == "" {
		return fallback
	}
	return *ptr
}

/*
========================
 Error Registry
========================
*/

type ErrorEntry struct {
	Count      int    `json:"count"`
	SampleBody string `json:"sample_body"`
}

type ErrorRegistry struct {
	mu     sync.RWMutex
	errors map[string]*ErrorEntry
}

func NewErrorRegistry() *ErrorRegistry {
	return &ErrorRegistry{
		errors: make(map[string]*ErrorEntry),
	}
}

func (r *ErrorRegistry) Add(key string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if e, ok := r.errors[key]; ok {
		e.Count++
		return
	}

	r.errors[key] = &ErrorEntry{
		Count:      1,
		SampleBody: string(body),
	}
}

func (r *ErrorRegistry) List() map[string]ErrorEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]ErrorEntry, len(r.errors))
	for k, v := range r.errors {
		out[k] = *v
	}
	return out
}

/*
========================
 DLQ Consumer (NO SESSION)
========================
*/

func runDLQConsumer(
	ctx context.Context,
	connStr string,
	queueName string,
	registry *ErrorRegistry,
) error {

	client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return err
	}

	receiver, err := client.NewReceiverForQueue(
		queueName,
		&azservicebus.ReceiverOptions{
			SubQueue: azservicebus.SubQueueDeadLetter,
		},
	)
	if err != nil {
		return err
	}
	defer receiver.Close(ctx)

	for {
		msgs, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			return err
		}

		if len(msgs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, msg := range msgs {
			reason := stringValue(msg.DeadLetterReason, "UNKNOWN_REASON")
			desc := stringValue(msg.DeadLetterErrorDescription, "UNKNOWN_ERROR")
			key := reason + " | " + desc

			registry.Add(key, msg.Body)
			_ = receiver.CompleteMessage(ctx, msg, nil)
		}
	}
}

/*
========================
 HTTP API
========================
*/

func errorAPI(registry *ErrorRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(registry.List())
	}
}

/*
========================
 main
========================
*/

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file")
	}

	var (
		connStr   = os.Getenv("SERVICEBUS_CONNECTION_STRING")
		queueName = "test_new_payroll_1"
	)

	if connStr == "" {
		log.Fatal("SERVICEBUS_CONNECTION_STRING is not set")
	}

	ctx := context.Background()
	registry := NewErrorRegistry()

	go func() {
		if err := runDLQConsumer(ctx, connStr, queueName, registry); err != nil {
			log.Fatal(err)
		}
	}()

	http.HandleFunc("/deadletter/errors", errorAPI(registry))
	log.Println("HTTP server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
