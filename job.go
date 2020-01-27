package sqsq

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Job struct {
	msg      *sqs.Message
	queueUrl *string
	queue    *Queue

	mu          sync.Mutex
	released    bool
	releaseChan chan bool
	success     bool
}

func NewJob(queue *Queue, queueUrl *string, msg *sqs.Message) *Job {
	return &Job{
		queue:    queue,
		queueUrl: queueUrl,
		msg:      msg,
		released: false,
		success:  false,

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
		_, err := j.queue.svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      j.queueUrl,
			ReceiptHandle: j.msg.ReceiptHandle,
		})
		if err != nil {
			j.queue.logger.Errorf("failed delete job: %v", err)
		}
	} else {
		_, err := j.queue.svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          j.queueUrl,
			ReceiptHandle:     j.msg.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
		if err != nil {
			j.queue.logger.Errorf("failed change to visible")
		}
	}

	j.released = true
	j.releaseChan <- true
}

func (j *Job) GetData() *string {
	return j.msg.Body
}

func (j *Job) Done() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.released {
		return
	}
	j.success = true
}
