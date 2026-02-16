package azbus2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-benchmark-new-payroll/internal/dto"
	"log"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"gorm.io/gorm"
)

type SessionReceiverOptions struct {
	SessionPool int
	BatchSize   int
	ProcessPool int
	RetryDelay  int
}

type Option func(*SessionReceiverOptions)

type SessionReceiver struct {
	ctx       context.Context
	wg        *sync.WaitGroup
	client    *azservicebus.Client
	queueName string
	db1       *gorm.DB
	db2       *gorm.DB
	options   SessionReceiverOptions
}

func NewSessionReceiver(ctx context.Context, wg *sync.WaitGroup, client *azservicebus.Client, queueName string, db1 *gorm.DB, db2 *gorm.DB, opts *SessionReceiverOptions) *SessionReceiver {
	// 1. กำหนดค่า Default
	defaultOpts := SessionReceiverOptions{
		SessionPool: 20,
		BatchSize:   5,
		ProcessPool: 1,
		RetryDelay:  5,
	}

	// 2. Apply options ที่กำหนดมา หากมี
	if opts != nil {
		if opts.SessionPool > 0 {
			defaultOpts.SessionPool = opts.SessionPool
		}
		if opts.BatchSize > 0 {
			defaultOpts.BatchSize = opts.BatchSize
		}
		if opts.ProcessPool > 0 {
			defaultOpts.ProcessPool = opts.ProcessPool
		}
		if opts.RetryDelay > 0 {
			defaultOpts.RetryDelay = opts.RetryDelay
		}
	}

	// 3. สร้าง Struct โดยใช้ opts ที่ได้มา
	return &SessionReceiver{
		ctx:       ctx,
		wg:        wg,
		client:    client,
		queueName: queueName,
		db1:       db1,
		db2:       db2,
		options:   defaultOpts,
	}
}

// RunDispatcher continuously accepts sessions from the queue and dispatches workers to process them
func (sr *SessionReceiver) RunDispatcher() {
	// Spawn worker (Initialization)
	workerCH := make(chan int, sr.options.SessionPool)
	for i := range make([]int, sr.options.SessionPool) {
		workerCH <- i + 1
	}

	for {
		select {
		case <-sr.ctx.Done():
			log.Printf("[%s] Shutting down Session Receiver. Waiting for active sessions to complete...", sr.queueName)
			sr.wg.Wait()
			log.Printf("[%s] All active sessions completed. Session Receiver stopped.", sr.queueName)
			return
		default:
		}

		select {
		case workerNo := <-workerCH:
			acceptSessionCtx, acceptSessionCancel := context.WithTimeout(sr.ctx, 5*time.Second)

			// sessionReceiver, err := sr.client.AcceptSessionForQueue(acceptSessionCtx, sr.queueName, "202205072D33E7BA048F", nil)
			sessionReceiver, err := sr.client.AcceptNextSessionForQueue(acceptSessionCtx, sr.queueName, nil)
			if err != nil {
				acceptSessionCancel()

				workerCH <- workerNo
				if errors.Is(err, context.DeadlineExceeded) {
					log.Printf("[%s] Worker<%d>: Session accept timed out. Retrying in %d seconds...", sr.queueName, workerNo, sr.options.RetryDelay)
				}
				time.Sleep(time.Duration(sr.options.RetryDelay) * time.Second)
				continue
			}

			sr.wg.Add(1)
			go sr.runSessionWorker(sessionReceiver, acceptSessionCancel, workerCH, workerNo)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// runSessionWorker processes messages from a single session
func (sr *SessionReceiver) runSessionWorker(sessionReceiver *azservicebus.SessionReceiver, acceptSessionCancel context.CancelFunc, workerCH chan int, workerNo int) {
	defer sr.wg.Done()
	defer func() { workerCH <- workerNo }()
	defer acceptSessionCancel()
	defer sessionReceiver.Close(sr.ctx)
	// defer log.Printf("[%s] Worker<%d>: %s [/]\n", sr.queueName, workerNo, sessionReceiver.SessionID())

	log.Printf("[%s] Worker<%d>: %s [ ]\n", sr.queueName, workerNo, sessionReceiver.SessionID())

	recvCtx, cancel := context.WithTimeout(sr.ctx, 10*time.Second)

	msgs, err := sessionReceiver.ReceiveMessages(recvCtx, sr.options.BatchSize, nil)
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("[%s] Worker<%d>: No messages, polling again... [/]\n", sr.queueName, workerNo)
			return
		}

		log.Printf("[%s] Receive error: %v", sr.queueName, err)
		return
	}

	defer log.Printf("[%s] Worker<%d>: %s | Messages: %d [/]\n", sr.queueName, workerNo, sessionReceiver.SessionID(), len(msgs))

	if len(msgs) == 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, sr.options.ProcessPool)

	for _, msg := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go sr.runMessageWorker(sem, &wg, sessionReceiver, msg)
	}

	wg.Wait()
}

// runMessageWorker handles a single message, applies business logic and updates Redis
func (sr *SessionReceiver) runMessageWorker(sem chan struct{}, wg *sync.WaitGroup, sessionReceiver *azservicebus.SessionReceiver, msg *azservicebus.ReceivedMessage) {
	defer wg.Done()
	defer func() { <-sem }()

	err := sr.handleMessage(msg)

	if err != nil {
		// Log: Failed/Abandon
		fmt.Println(err)

		// sessionReceiver.AbandonMessage(sr.ctx, msg, nil)
		reason := "PROCESSING_FAILED"
		desc := err.Error()
		sessionReceiver.DeadLetterMessage(
			sr.ctx,
			msg,
			&azservicebus.DeadLetterOptions{
				Reason:           &reason,
				ErrorDescription: &desc,
			},
		)

		return
	}

	// Log: Success/Complete
	sessionReceiver.CompleteMessage(sr.ctx, msg, nil)
}

// handleMessage contains the actual business logic for processing a single message
func (sr *SessionReceiver) handleMessage(msg *azservicebus.ReceivedMessage) error {
	// Check for nil message early
	if msg == nil {
		return fmt.Errorf("received nil message")
	}

	var body dto.CompareMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("invalid message json: %w", err)
	}
	if err := body.Validate(); err != nil {
		return err
	}

	// Database Spare
	var slip1Records []struct {
		YearMonth string `gorm:"column:year_month"`
		// ExtraCodeFlag string  `gorm:"column:extra_code_flag"`
		// Period        string  `gorm:"column:period"`
		// DomainCode    string  `gorm:"column:domain_code"`
		// ChannelCode   string  `gorm:"column:channel_code"`
		EmployeeID              string  `gorm:"column:employee_id"`
		BranchID                string  `gorm:"column:branch_id"`
		TotalSalary             float64 `gorm:"column:total_salary"`
		InstanceServerID        string  `gorm:"column:instance_server_id"`
		InstanceServerChannelID string  `gorm:"column:instance_server_channel_id"`
		MasterSalaryReportID    string  `gorm:"column:master_salary_report_id"`
		SalarySplitFlag         string  `gorm:"column:salary_split_flag"`
	}
	sr.db1.Raw(`
		SELECT _slip.master_salary_month as 'year_month'
		, _slip.employee_id
		, _slip.total_salary
		, _slip.instance_server_id
		, _slip.instance_server_channel_id
		, _slip.master_salary_report_id
		, _report.salary_split_flag
		FROM `+body.DbName+`.payroll_master_salary_slip _slip
		INNER JOIN `+body.DbName+`.payroll_master_salary_report _report ON (_report.master_salary_report_id = _slip.master_salary_report_id)
		WHERE _slip.employee_id = ?
		AND _slip.master_salary_month = ?
	`, body.EmployeeID, body.YearMonth).Scan(&slip1Records)
	if len(slip1Records) == 0 {
		return fmt.Errorf("no salary slip found in spare database for employee_id: %s, year_month: %s", body.EmployeeID, body.YearMonth)
	}
	fmt.Println(slip1Records)

	// Database Production
	var slip2Records []struct {
		YearMonth            string  `gorm:"column:year_month"`
		EmployeeID           string  `gorm:"column:employee_id"`
		TotalSalary          float64 `gorm:"column:total_salary"`
		MasterSalaryReportID string  `gorm:"column:master_salary_report_id"`
	}
	sr.db2.Raw(`
		SELECT master_salary_month as 'year_month'
		, employee_id
		, total_salary
		, master_salary_report_id
		FROM `+body.DbName+`.payroll_master_salary_slip
		WHERE employee_id = ?
		AND master_salary_month = ?
	`, body.EmployeeID, body.YearMonth).Scan(&slip2Records)
	if len(slip2Records) == 0 {
		return fmt.Errorf("no salary slip found in production database for employee_id: %s, year_month: %s", body.EmployeeID, body.YearMonth)
	}

	// Query Data (DomainCode, ChannelCode, Extracode) from Database Spare
	var result struct {
		DomainCode    string `gorm:"column:domain_code"`
		ChannelCode   string `gorm:"column:channel_code"`
		ExtraCodeFlag string `gorm:"column:extra_code_flag"`
	}
	sr.db1.Raw(`
		SELECT _sv.instance_server_code as domain_code
		, _ch.instance_server_channel_code as channel_code
		, IF(_cc.instance_server_channel_id IS NULL, 'N', 'Y') AS extra_code_flag
		FROM hms_api.sys_instance_server _sv
		INNER JOIN hms_api.sys_instance_server_channel _ch ON (_ch.instance_server_id = _sv.instance_server_id)
		LEFT JOIN (
			SELECT instance_server_channel_id
			FROM hms_api.ai_condition_config
			WHERE server_id = ?
			AND instance_server_id = ?
			AND instance_server_channel_id = ?
			GROUP BY instance_server_channel_id
		) _cc ON (_cc.instance_server_channel_id = _ch.instance_server_channel_id)
		WHERE _sv.server_id = ?
		AND _sv.instance_server_id = ?
		AND _ch.instance_server_channel_id = ?
	`, "100002", slip1Records[0].InstanceServerID, slip1Records[0].InstanceServerChannelID, "100002", slip1Records[0].InstanceServerID, slip1Records[0].InstanceServerChannelID).Scan(&result)
	if result.DomainCode == "" || result.ChannelCode == "" {
		return fmt.Errorf("failed to fetch domain/channel/extracode for employee_id: %s, year_month: %s", body.EmployeeID, body.YearMonth)
	}

	var resultImportLog struct {
		ImportLogFundFlag     string `gorm:"column:import_log_fund_flag"`
		ImportLogNormalFlag   string `gorm:"column:import_log_normal_flag"`
		ImportLogWorktimeFlag string `gorm:"column:import_log_worktime_flag"`
	}
	sr.db1.Raw(`
		SELECT
		CASE
			WHEN COUNT(CASE WHEN import_log_type = 'fund' THEN 1 END) > 0 THEN 'Y'
			ELSE 'N'
		END as import_log_fund_flag,
		CASE
			WHEN COUNT(CASE WHEN import_log_type = 'normal' THEN 1 END) > 0 THEN 'Y'
			ELSE 'N'
		END as import_log_normal_flag,
		CASE
			WHEN COUNT(CASE WHEN import_log_type = 'worktime' THEN 1 END) > 0 THEN 'Y'
			ELSE 'N'
		END as import_log_worktime_flag
		FROM `+body.DbName+`.payroll_import_log
		WHERE server_id = ?
		AND instance_server_id = ?
		AND instance_server_channel_id = ?
		AND import_log_type IN ('fund', 'normal', 'worktime')
		AND import_month = ?
		GROUP BY instance_server_channel_id, import_month
	`, "100002", slip1Records[0].InstanceServerID, slip1Records[0].InstanceServerChannelID, body.YearMonth).Scan(&resultImportLog)
	if resultImportLog.ImportLogFundFlag == "" || resultImportLog.ImportLogNormalFlag == "" || resultImportLog.ImportLogWorktimeFlag == "" {
		resultImportLog.ImportLogFundFlag = "N"
		resultImportLog.ImportLogNormalFlag = "N"
		resultImportLog.ImportLogWorktimeFlag = "N"
	}

	var resultMasterEmployee struct {
		MasterEmployeeFlag string `gorm:"column:master_employee_flag"`
	}
	sr.db1.Raw(`
		SELECT CASE WHEN master_employee_id IS NOT NULL THEN 'Y' ELSE 'N' END AS master_employee_flag
		FROM `+body.DbName+`.payroll_master_employee
		WHERE server_id = ?
		AND instance_server_id = ?
		AND instance_server_channel_id = ?
		AND master_salary_report_id = ?
		AND employee_id = ?
		LIMIT 1
	`, "100002", slip1Records[0].InstanceServerID, slip1Records[0].InstanceServerChannelID, slip1Records[0].MasterSalaryReportID, slip1Records[0].EmployeeID).Scan(&resultMasterEmployee)
	if resultMasterEmployee.MasterEmployeeFlag == "" {
		resultMasterEmployee.MasterEmployeeFlag = "N"
	}

	var resultMasterEmloyeeConfig struct {
		MasterEmployeeConfigFlag string `gorm:"column:master_employee_config_flag"`
	}
	sr.db1.Raw(`
		SELECT CASE WHEN master_employee_config_id IS NOT NULL THEN 'Y' ELSE 'N' END AS master_employee_config_flag
		FROM `+body.DbName+`.payroll_master_employee_config
		WHERE employee_id = ?
		AND master_salary_month = ?
		LIMIT 1
	`, slip1Records[0].EmployeeID, body.YearMonth).Scan(&resultMasterEmloyeeConfig)
	if resultMasterEmloyeeConfig.MasterEmployeeConfigFlag == "" {
		resultMasterEmloyeeConfig.MasterEmployeeConfigFlag = "N"
	}

	var resultMasterCompanyConfig struct {
		MasterCompanyConfigFlag string `gorm:"column:master_company_config_flag"`
	}
	sr.db1.Raw(`
		SELECT CASE WHEN master_company_config_id IS NOT NULL THEN 'Y' ELSE 'N' END AS master_company_config_flag
		FROM `+body.DbName+`.payroll_master_company_config
		WHERE server_id = ?
		AND instance_server_id = ?
		AND instance_server_channel_id = ?
		AND master_salary_month = ?
		LIMIT 1
	`, "100002", slip1Records[0].InstanceServerID, slip1Records[0].InstanceServerChannelID, body.YearMonth).Scan(&resultMasterCompanyConfig)
	if resultMasterCompanyConfig.MasterCompanyConfigFlag == "" {
		resultMasterCompanyConfig.MasterCompanyConfigFlag = "N"
	}

	var resultMasterBranchSsoConfig struct {
		MasterBranchSsoConfigFlag string `gorm:"column:master_branch_sso_config_flag"`
	}
	sr.db1.Raw(`
		SELECT CASE WHEN master_branch_sso_config_id IS NOT NULL THEN 'Y' ELSE 'N' END AS master_branch_sso_config_flag
		FROM `+body.DbName+`.payroll_master_branch_sso_config
		WHERE master_salary_month = ?
		AND branch_id = ?
		LIMIT 1
	`, body.YearMonth, slip1Records[0].BranchID).Scan(&resultMasterBranchSsoConfig)
	if resultMasterBranchSsoConfig.MasterBranchSsoConfigFlag == "" {
		resultMasterBranchSsoConfig.MasterBranchSsoConfigFlag = "N"
	}

	// Log MySQL Full
	logRecord := dto.LogRecord{
		YearMonth:                 body.YearMonth,
		DomainCode:                result.DomainCode,
		ChannelCode:               result.ChannelCode,
		Period:                    "Full",
		ExtraCodeFlag:             result.ExtraCodeFlag,
		ImportLogFundFlag:         resultImportLog.ImportLogFundFlag,
		ImportLogNormalFlag:       resultImportLog.ImportLogNormalFlag,
		ImportLogWorktimeFlag:     resultImportLog.ImportLogWorktimeFlag,
		MasterEmployeeFlag:        resultMasterEmployee.MasterEmployeeFlag,
		MasterEmployeeConfigFlag:  resultMasterEmloyeeConfig.MasterEmployeeConfigFlag,
		MasterCompanyConfigFlag:   resultMasterCompanyConfig.MasterCompanyConfigFlag,
		MasterBranchSsoConfigFlag: resultMasterBranchSsoConfig.MasterBranchSsoConfigFlag,
		EmployeeID:                body.EmployeeID,
		TotalSalaryOld:            slip2Records[0].TotalSalary,
		TotalSalaryNew:            slip1Records[0].TotalSalary,
		TotalVariance:             slip1Records[0].TotalSalary - slip2Records[0].TotalSalary,
	}
	if err := sr.CreateLogMySQL(&logRecord); err != nil {
		return err
	}

	// Check if split salary exists
	if slip1Records[0].SalarySplitFlag == "Y" {
		// Database Spare
		var splitSlip1Records []struct {
			YearMonth   string  `gorm:"column:year_month"`
			Period      string  `gorm:"column:master_salary_split_seq"`
			EmployeeID  string  `gorm:"column:employee_id"`
			TotalSalary float64 `gorm:"column:total_salary"`
		}
		sr.db1.Raw(`
			SELECT _slip.master_salary_month as 'year_month'
			, _report.master_salary_split_seq
			, _slip.employee_id
			, _slip.total_salary
			FROM `+body.DbName+`.payroll_master_salary_split_slip _slip
			INNER JOIN `+body.DbName+`.payroll_master_salary_split_report _report ON (_report.master_salary_split_report_id = _slip.master_salary_split_report_id)
			WHERE _report.master_salary_report_id = ?
			AND _slip.employee_id = ?
			ORDER BY _report.master_salary_split_seq
		`, slip1Records[0].MasterSalaryReportID, slip1Records[0].EmployeeID).Scan(&splitSlip1Records)
		if len(splitSlip1Records) == 0 {
			return fmt.Errorf("no salary split slip found in spare database for employee_id: %s, year_month: %s", body.EmployeeID, body.YearMonth)
		}

		// Database Production
		var splitSlip2Records []struct {
			YearMonth   string  `gorm:"column:year_month"`
			Period      string  `gorm:"column:master_salary_split_seq"`
			EmployeeID  string  `gorm:"column:employee_id"`
			TotalSalary float64 `gorm:"column:total_salary"`
		}
		sr.db2.Raw(`
			SELECT _slip.master_salary_month as 'year_month'
			, _report.master_salary_split_seq
			, _slip.employee_id
			, _slip.total_salary
			FROM `+body.DbName+`.payroll_master_salary_split_slip _slip
			INNER JOIN `+body.DbName+`.payroll_master_salary_split_report _report ON (_report.master_salary_split_report_id = _slip.master_salary_split_report_id)
			WHERE _report.master_salary_report_id = ?
			AND _slip.employee_id = ?
			ORDER BY _report.master_salary_split_seq
		`, slip2Records[0].MasterSalaryReportID, slip2Records[0].EmployeeID).Scan(&splitSlip2Records)
		if len(splitSlip2Records) == 0 {
			return fmt.Errorf("no salary split slip found in production database for employee_id: %s, year_month: %s", body.EmployeeID, body.YearMonth)
		}

		// Log MySQL Split
		for _, splitSlip1Record := range splitSlip1Records {
			for _, splitSlip2Record := range splitSlip2Records {
				if splitSlip1Record.YearMonth == splitSlip2Record.YearMonth && splitSlip1Record.Period == splitSlip2Record.Period && splitSlip1Record.EmployeeID == splitSlip2Record.EmployeeID {
					logRecordSplit := dto.LogRecord{
						YearMonth:                 body.YearMonth,
						DomainCode:                result.DomainCode,
						ChannelCode:               result.ChannelCode,
						Period:                    splitSlip1Record.Period,
						ExtraCodeFlag:             result.ExtraCodeFlag,
						ImportLogFundFlag:         resultImportLog.ImportLogFundFlag,
						ImportLogNormalFlag:       resultImportLog.ImportLogNormalFlag,
						ImportLogWorktimeFlag:     resultImportLog.ImportLogWorktimeFlag,
						MasterEmployeeFlag:        resultMasterEmployee.MasterEmployeeFlag,
						MasterEmployeeConfigFlag:  resultMasterEmloyeeConfig.MasterEmployeeConfigFlag,
						MasterCompanyConfigFlag:   resultMasterCompanyConfig.MasterCompanyConfigFlag,
						MasterBranchSsoConfigFlag: resultMasterBranchSsoConfig.MasterBranchSsoConfigFlag,
						EmployeeID:                body.EmployeeID,
						TotalSalaryOld:            splitSlip2Record.TotalSalary,
						TotalSalaryNew:            splitSlip1Record.TotalSalary,
						TotalVariance:             splitSlip1Record.TotalSalary - splitSlip2Record.TotalSalary,
					}
					if err := sr.CreateLogMySQL(&logRecordSplit); err != nil {
						return err
					}
				}
			}
		}
	}

	// Successfully processed the message
	return nil
}

func (sr *SessionReceiver) CreateLogMySQL(log *dto.LogRecord) error {
	query := `INSERT INTO hms_payroll.audit_salary_logs
              (
                ` + "`year_month`" + `,
                ` + "`domain_code`" + `,
                ` + "`channel_code`" + `,
                ` + "`period`" + `,
                ` + "`extra_code_flag`" + `,
                ` + "`employee_id`" + `,
                ` + "`total_salary_old`" + `,
                ` + "`total_salary_new`" + `,
                ` + "`total_variance`" + `,
                ` + "`import_log_fund_flag`" + `,
                ` + "`import_log_normal_flag`" + `,
                ` + "`import_log_worktime_flag`" + `,
                ` + "`master_employee_flag`" + `,
                ` + "`master_employee_config_flag`" + `,
                ` + "`master_company_config_flag`" + `,
                ` + "`master_branch_sso_config_flag`" + `
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE
                ` + "`total_salary_old`" + ` = VALUES(` + "`total_salary_old`" + `),
                ` + "`total_salary_new`" + ` = VALUES(` + "`total_salary_new`" + `),
                ` + "`total_variance`" + ` = VALUES(` + "`total_variance`" + `),
                ` + "`domain_code`" + ` = VALUES(` + "`domain_code`" + `),
                ` + "`channel_code`" + ` = VALUES(` + "`channel_code`" + `),
                ` + "`extra_code_flag`" + ` = VALUES(` + "`extra_code_flag`" + `),
                ` + "`import_log_fund_flag`" + ` = VALUES(` + "`import_log_fund_flag`" + `),
                ` + "`import_log_normal_flag`" + ` = VALUES(` + "`import_log_normal_flag`" + `),
                ` + "`import_log_worktime_flag`" + ` = VALUES(` + "`import_log_worktime_flag`" + `),
                ` + "`master_employee_flag`" + ` = VALUES(` + "`master_employee_flag`" + `),
                ` + "`master_employee_config_flag`" + ` = VALUES(` + "`master_employee_config_flag`" + `),
                ` + "`master_company_config_flag`" + ` = VALUES(` + "`master_company_config_flag`" + `),
                ` + "`master_branch_sso_config_flag`" + ` = VALUES(` + "`master_branch_sso_config_flag`" + `);`

	err := sr.db1.Exec(query,
		log.YearMonth,
		log.DomainCode,
		log.ChannelCode,
		log.Period,
		log.ExtraCodeFlag,
		log.EmployeeID,
		log.TotalSalaryOld,
		log.TotalSalaryNew,
		log.TotalVariance,
		log.ImportLogFundFlag,
		log.ImportLogNormalFlag,
		log.ImportLogWorktimeFlag,
		log.MasterEmployeeFlag,
		log.MasterEmployeeConfigFlag,
		log.MasterCompanyConfigFlag,
		log.MasterBranchSsoConfigFlag,
	).Error

	return err
}
