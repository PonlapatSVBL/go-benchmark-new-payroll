package main

import (
	"context"
	"go-benchmark-new-payroll/internal/azbus1"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	sbConnStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")

	// New azure service bus client
	client, err := azservicebus.NewClientFromConnectionString(sbConnStr, nil)
	if err != nil {
		log.Fatalf("Failed to create Service Bus client: %v", err)
	}
	defer client.Close(ctx)

	// Create sender
	sender, err := client.NewSender("test_new_payroll_2", nil)
	if err != nil {
		log.Fatal(err)
	}

	// Create session receiver
	receiver1Opts := azbus1.SessionReceiverOptions{
		SessionPool: 75,
		BatchSize:   5,
		ProcessPool: 5,
	}
	receiver1 := azbus1.NewSessionReceiver(ctx, &wg, client, sender, "test_new_payroll_1", &receiver1Opts)

	// Run session receiver
	go receiver1.RunDispatcher()

	// Wait for OS interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	log.Println("Received interrupt signal. Initiating graceful shutdown...")

	// Cancel context to notify goroutines to stop
	cancel()

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("All services shut down completely.")
}
