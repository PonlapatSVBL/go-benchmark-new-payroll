package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go-benchmark-new-payroll/internal/dto"
	"log"
	"os"
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
		log.Println("Error loading .env file")
	}

	// เชื่อมต่อ Database (GORM)
	dbHost := os.Getenv("MYSQL_HOST")
	dbUser := os.Getenv("MYSQL_USER")
	dbPass := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")

	cfg := myConfig.Config{
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

	dsn := cfg.FormatDSN()

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(25)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// สร้าง Service Bus Client
	sbConnStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	queueName := "test_new_payroll_1"

	sbClient, err := azservicebus.NewClientFromConnectionString(sbConnStr, nil)
	if err != nil {
		log.Fatal(err)
	}
	sender, err := sbClient.NewSender(queueName, nil)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// ดึงรายชื่อ Database ที่ต้องการส่งไปคำนวณเงินเดือน
	var dbNames []string
	db.Raw("SELECT code FROM hms_api.sys_list_of_value WHERE list_type = 'inst_dbn' ORDER BY code LIMIT 5 OFFSET 0").Scan(&dbNames)

	// เตรียม Worker Pool สำหรับส่ง ServiceBus (ป้องกันคอขวด)
	msgChan := make(chan dto.PayrollMessage, 5000)
	var wgSender sync.WaitGroup

	// สร้าง 50 Workers สำหรับการส่งข้อมูล
	for i := 0; i < 50; i++ {
		wgSender.Add(1)
		go func() {
			defer wgSender.Done()
			workerSendToSB(ctx, sender, msgChan)
		}()
	}

	// เริ่มประมวลผลแยกตาม Database
	var wgDB sync.WaitGroup
	for _, name := range dbNames {
		wgDB.Add(1)
		go func(n string) {
			defer wgDB.Done()
			processDatabase(db, n, msgChan)
		}(name)
	}

	wgDB.Wait()
	close(msgChan)
	wgSender.Wait()
}

func processDatabase(db *gorm.DB, dbName string, msgChan chan<- dto.PayrollMessage) {
	var salaryMonthRecords []struct {
		MasterSalaryReportID string `gorm:"column:master_salary_report_id"`
		MasterSalaryMonth    string `gorm:"column:master_salary_month"`
		SalarySplitFlag      string `gorm:"column:salary_split_flag"`
	}
	db.Raw("SELECT master_salary_report_id, master_salary_month, salary_split_flag FROM " + dbName + ".payroll_master_salary_report WHERE master_salary_month <= '2025-12' ORDER BY master_salary_month DESC").Scan(&salaryMonthRecords)

	for _, record := range salaryMonthRecords {
		processMonth(db, dbName, record.MasterSalaryReportID, msgChan)
	}
}

func processMonth(db *gorm.DB, dbName string, masterSalaryReportID string, msgChan chan<- dto.PayrollMessage) {
	var slipRecords []struct {
		EmployeeID              string `gorm:"column:employee_id"`
		MasterSalaryMonth       string `gorm:"column:master_salary_month"`
		InstanceServerID        string `gorm:"column:instance_server_id"`
		InstanceServerChannelID string `gorm:"column:instance_server_channel_id"`
	}
	db.Raw("SELECT employee_id, master_salary_month, instance_server_id, instance_server_channel_id FROM "+dbName+".payroll_master_salary_slip WHERE master_salary_report_id = ?", masterSalaryReportID).Scan(&slipRecords)

	for _, slip := range slipRecords {
		encodedEmployeeID := base64.StdEncoding.EncodeToString([]byte(slip.EmployeeID))
		encodedInstanceServerID := base64.StdEncoding.EncodeToString([]byte(slip.InstanceServerID))
		encodedInstanceServerChannelID := base64.StdEncoding.EncodeToString([]byte(slip.InstanceServerChannelID))

		msg := dto.PayrollMessage{
			Compgrp:                 "hrs",
			Comp:                    "calculation_normal",
			Action:                  "calculate_payroll",
			YearMonth:               slip.MasterSalaryMonth,
			EmployeeID:              encodedEmployeeID,
			CalculatePerson:         "calculate_person",
			CalculateTo:             "NOW",
			InstanceServerID:        encodedInstanceServerID,
			InstanceServerChannelID: encodedInstanceServerChannelID,
			IdentifyUserID:          "MjAyMjExMDJFQzRGNUJDNTZGRjg=",
			LanguageCode:            "TH",
			UserName:                "",
			UserPsw:                 "",
			Url:                     "https://hms-php-core-payroll.azurewebsites.net/api-web.php",
			Schema:                  dbName,
		}
		msgChan <- msg
		fmt.Printf("Enqueued message for EmployeeID: %s, Month: %s\n", slip.EmployeeID, slip.MasterSalaryMonth)
	}
}

func workerSendToSB(ctx context.Context, sender *azservicebus.Sender, msgChan <-chan dto.PayrollMessage) {
	const maxBatchSize = 100 // ส่งทีละ 100 รายการต่อ 1 Network Request

	for {
		batch, err := sender.NewMessageBatch(ctx, nil)
		if err != nil {
			log.Printf("Failed to create batch: %v", err)
			continue
		}

		count := 0
		// พยายามดึงข้อมูลจาก channel มาใส่ batch ให้ได้มากที่สุด
		for count < maxBatchSize {
			msg, ok := <-msgChan
			if !ok {
				// ถ้า channel ปิดแล้ว และมีข้อมูลค้างใน batch ให้ส่งอันสุดท้าย
				if count > 0 {
					sender.SendMessageBatch(ctx, batch, nil)
				}
				return
			}

			payload, _ := json.Marshal(msg)
			err := batch.AddMessage(&azservicebus.Message{Body: payload, SessionID: &msg.Schema}, nil)
			if err != nil {
				// ถ้า batch เต็มก่อนถึง 100 ให้หยุดแล้วส่ง
				break
			}
			count++
		}

		if count > 0 {
			if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
				log.Printf("Failed to send batch: %v", err)
			}
		}
	}
}
