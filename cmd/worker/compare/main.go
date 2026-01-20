package main

import (
	"context"
	"go-benchmark-new-payroll/internal/azbus2"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	myConfig "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	// เชื่อมต่อ Database (GORM)
	dbHost := os.Getenv("MYSQL_HOST")
	dbUser := os.Getenv("MYSQL_USER")
	dbPass := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")

	cfg1 := myConfig.Config{
		User:   dbUser,
		Passwd: dbPass,
		Net:    "tcp",
		Addr:   dbHost + ":3306",
		DBName: dbName,
		Params: map[string]string{
			"charset":              "utf8mb4",
			"allowNativePasswords": "true",
		},
		ParseTime: true,
		Loc:       time.Local,
	}

	dsn1 := cfg1.FormatDSN()

	db1, err := gorm.Open(mysql.Open(dsn1), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	sqlDB1, _ := db1.DB()
	sqlDB1.SetMaxOpenConns(50)
	sqlDB1.SetMaxIdleConns(25)
	sqlDB1.SetConnMaxLifetime(time.Hour)

	// เชื่อมต่อ Database Read (GORM)
	dbHostRead := os.Getenv("MYSQL_HOST_READ")
	dbUserRead := os.Getenv("MYSQL_USER_READ")
	dbPassRead := os.Getenv("MYSQL_PASSWORD_READ")
	dbNameRead := os.Getenv("MYSQL_DB_READ")
	cfg2 := myConfig.Config{
		User:   dbUserRead,
		Passwd: dbPassRead,
		Net:    "tcp",
		Addr:   dbHostRead + ":3306",
		DBName: dbNameRead,
		Params: map[string]string{
			"charset":              "utf8mb4",
			"allowNativePasswords": "true",
		},
		ParseTime: true,
		Loc:       time.Local,
	}

	dsn2 := cfg2.FormatDSN()

	db2, err := gorm.Open(mysql.Open(dsn2), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	sqlDB2, _ := db2.DB()
	sqlDB2.SetMaxOpenConns(50)
	sqlDB2.SetMaxIdleConns(25)
	sqlDB2.SetConnMaxLifetime(time.Hour)

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

	// Create session receiver
	receiver1Opts := azbus2.SessionReceiverOptions{
		SessionPool: 100,
		BatchSize:   10,
		ProcessPool: 5,
	}
	receiver1 := azbus2.NewSessionReceiver(ctx, &wg, client, "test_new_payroll_2", db1, db2, &receiver1Opts)

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
