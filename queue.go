package sqsq

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Queue struct {
	svc            *sqs.SQS
	queueNameToUrl map[string]*string
	logger         Logger
}

type Config struct {
	Debug  bool
	Logger Logger
}

func NewQueue(awsConfig *aws.Config, config *Config) (*Queue, error) {
	sess := session.New(awsConfig)

	svc := sqs.New(sess)

	var logger Logger
	if config != nil && config.Logger != nil {
		logger = config.Logger
	} else {
		debug := false
		if config != nil && config.Debug {
			debug = config.Debug
		}
		logger = NewDefaultLogger(debug)
	}

	return &Queue{
		svc:            svc,
		queueNameToUrl: map[string]*string{},
		logger:         logger,
	}, nil
}

// UseQueue Cache a queue name to url map.
func (q *Queue) UseQueue(name string) error {
	u, err := q.svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return fmt.Errorf("failed get queue url[%v]: %w", name, err)
	}

	q.queueNameToUrl[name] = u.QueueUrl
	return nil
}

func (q *Queue) PutJob(queueName string, body string, delay int64) error {
	u, ok := q.queueNameToUrl[queueName]
	if !ok {
		return fmt.Errorf("Bad queue name: %v", queueName)
	}

	in := &sqs.SendMessageInput{
		MessageBody:  aws.String(body),
		QueueUrl:     u,
		DelaySeconds: aws.Int64(delay),
	}
	if _, err := q.svc.SendMessage(in); err != nil {
		return err
	}

	return nil
}

func (q *Queue) watchQueueProcess(queueUrl *string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) (int, error) {
	q.logger.Debugf("sqs receive message request: %s", *queueUrl)
	in := &sqs.ReceiveMessageInput{
		QueueUrl:            queueUrl,
		MaxNumberOfMessages: aws.Int64(concurrency),
		VisibilityTimeout:   aws.Int64(visibilityTimeout),
		WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
	}

	res, err := q.svc.ReceiveMessage(in)
	if err != nil {
		return 0, err
	}

	wg := &sync.WaitGroup{}
	for _, msg := range res.Messages {
		wg.Add(1)
		job := NewJob(q, queueUrl, msg)
		ch <- job

		go func() {
			<-job.releaseChan
			wg.Done()
		}()
	}
	wg.Wait()

	return len(res.Messages), nil
}

func (q *Queue) WatchQueue(ctx context.Context, queueName string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) error {
	u, ok := q.queueNameToUrl[queueName]
	if !ok {
		return fmt.Errorf("Bad queue name: %s", queueName)
	}

	rcvChan := make(chan bool)

	go func() {
		rcvChan <- true
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rcvChan:
			_, err := q.watchQueueProcess(u, concurrency, visibilityTimeout, waitTimeSeconds, ch)
			if err != nil {
				q.logger.Debugf("failed watchQueueProcess: %v", err)
			}
			go func() {
				rcvChan <- true
			}()
		}
	}
}

func (q *Queue) DrainQueue(ctx context.Context, queueName string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) error {
	q.logger.Debugf("start drain queue: %s", queueName)

	u, ok := q.queueNameToUrl[queueName]
	if !ok {
		return fmt.Errorf("Bad queue name: %s", queueName)
	}

	rcvChan := make(chan bool)

	go func() {
		rcvChan <- true
	}()

	doneChan := make(chan bool)
	errChan := make(chan error)

	defer q.logger.Debugf("done drain queue: %s", queueName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rcvChan:
			go func() {
				n, err := q.watchQueueProcess(u, concurrency, visibilityTimeout, waitTimeSeconds, ch)
				if err != nil {
					errChan <- err
					return
				}
				q.logger.Debugf("queue message num = %d", n)
				if n <= 0 {
					doneChan <- true
					return
				}
				rcvChan <- true
			}()
		case err := <-errChan:
			return err
		case <-doneChan:
			return nil
		}
	}
}
