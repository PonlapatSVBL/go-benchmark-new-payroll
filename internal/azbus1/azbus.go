package azbus1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go-benchmark-new-payroll/internal/dto"
	"go-benchmark-new-payroll/internal/httpclient"
	"log"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
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
	sender    *azservicebus.Sender
	queueName string
	options   SessionReceiverOptions
}

func NewSessionReceiver(ctx context.Context, wg *sync.WaitGroup, client *azservicebus.Client, sender *azservicebus.Sender, queueName string, opts *SessionReceiverOptions) *SessionReceiver {
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
		sender:    sender,
		queueName: queueName,
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

	var body dto.PayrollMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("invalid message json: %w", err)
	}
	if err := body.Validate(); err != nil {
		return err
	}

	decodedEmployeeID, err := base64.StdEncoding.DecodeString(body.EmployeeID)
	if err != nil {
		return fmt.Errorf("failed to decode employee ID: %w", err)
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	statusCode, response, err := httpclient.PostRequestPayroll(body.Url, payload)
	if err != nil {
		return err
	} else if statusCode != 200 || response.Code != "200" {
		respBytes, _ := json.Marshal(response)
		return fmt.Errorf(string(respBytes))
	}

	bodyCompare := dto.CompareMessage{
		DbName:     body.Schema,
		EmployeeID: string(decodedEmployeeID),
		YearMonth:  body.YearMonth,
	}

	azMsg, err := json.Marshal(bodyCompare)
	if err != nil {
		return err
	}

	if err = sr.sender.SendMessage(sr.ctx, &azservicebus.Message{
		Body:      azMsg,
		SessionID: msg.SessionID,
	}, nil); err != nil {
		return err
	}

	// Successfully processed the message
	return nil
}
