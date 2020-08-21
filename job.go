package sqsq

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Job struct {
	msg      *sqs.Message
	queueUrl *string
	service  *Service

	mu          sync.Mutex
	released    bool
	releaseChan chan bool
	success     bool
	postponed   bool
}

func NewJob(queue *Service, queueUrl *string, msg *sqs.Message) *Job {
	return &Job{
		service:   queue,
		queueUrl:  queueUrl,
		msg:       msg,
		released:  false,
		success:   false,
		postponed: false,

		releaseChan: make(chan bool),
	}
}

func (j *Job) Release() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.released {
		return
	}
	if j.success {
		_, err := j.service.svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      j.queueUrl,
			ReceiptHandle: j.msg.ReceiptHandle,
		})
		if err != nil {
			j.service.logger.Errorf("failed delete job: %v", err)
		}
		j.service.logger.Printf("success job. delete message[id=%v].", *j.msg.MessageId)

	} else {
		if j.postponed {
			// nop
		} else {
			// ASAP retry
			err := j.changeVisibilityTimeout(0)
			if err != nil {
				j.service.logger.Errorf("failed change to visible")
			}
			j.service.logger.Printf("change visibility timeout to zero")
		}
	}

	j.released = true
	j.releaseChan <- true
}

func (j *Job) GetData() *string {
	return j.msg.Body
}

func (j *Job) Postpone(d time.Duration) error {
	j.postponed = true
	return j.changeVisibilityTimeout(int64(d.Seconds()))
}

func (j *Job) changeVisibilityTimeout(t int64) error {
	_, err := j.service.svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          j.queueUrl,
		ReceiptHandle:     j.msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(t),
	})
	if err != nil {
		return err
	}
	return nil
}

func (j *Job) Done() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.released {
		return
	}
	j.success = true
}
