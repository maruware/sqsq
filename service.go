package sqsq

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Service struct {
	svc            *sqs.SQS
	queueNameToUrl map[string]*string
	logger         Logger
}

type Config struct {
	Debug  bool
	Logger Logger
}

func New(awsConfig *aws.Config, config *Config) (*Service, error) {
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

	return &Service{
		svc:            svc,
		queueNameToUrl: map[string]*string{},
		logger:         logger,
	}, nil
}

// UseQueue Cache a queue name to url map.
func (q *Service) UseQueue(name string) error {
	u, err := q.svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return fmt.Errorf("failed get queue url[%v]: %w", name, err)
	}

	q.queueNameToUrl[name] = u.QueueUrl
	return nil
}

func (q *Service) PutJob(queueName string, body string, delay int64) error {
	u, err := q.getQueueUrl(queueName)
	if err != nil {
		return err
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

func (q *Service) watchQueueProcess(queueUrl *string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) (int, error) {
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

func (q *Service) WatchQueue(ctx context.Context, queueName string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) error {
	u, err := q.getQueueUrl(queueName)
	if err != nil {
		return err
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

func (q *Service) DrainQueue(ctx context.Context, queueName string, concurrency, visibilityTimeout, waitTimeSeconds int64, ch chan<- *Job) error {
	q.logger.Debugf("start drain queue: %s", queueName)

	u, err := q.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	rcvChan := make(chan bool)

	go func() {
		rcvChan <- true
	}()

	doneChan := make(chan error)

	defer q.logger.Debugf("done drain queue: %s", queueName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rcvChan:
			go func() {
				n, err := q.watchQueueProcess(u, concurrency, visibilityTimeout, waitTimeSeconds, ch)
				if err != nil {
					doneChan <- err
					return
				}
				q.logger.Debugf("queue message num = %d", n)
				if n <= 0 {
					doneChan <- nil
					return
				}
				rcvChan <- true
			}()
		case err := <-doneChan:
			return err
		}
	}
}

func (q *Service) getQueueUrl(name string) (*string, error) {
	u, ok := q.queueNameToUrl[name]
	if !ok {
		return nil, fmt.Errorf("Bad queue name: %s", name)
	}
	return u, nil
}

func (q *Service) getQueueAttributes(queueName string, attributeNames []*string) (*sqs.GetQueueAttributesOutput, error) {
	u, err := q.getQueueUrl(queueName)
	if err != nil {
		return nil, err
	}

	return q.svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       u,
		AttributeNames: attributeNames,
	})
}

func (q *Service) getIntAttribute(queueName, attr string) (int, error) {
	r, err := q.getQueueAttributes(queueName, []*string{
		aws.String(attr),
	})

	if err != nil {
		return 0, err
	}

	v := r.Attributes[attr]
	l, err := strconv.Atoi(*v)
	if err != nil {
		return 0, err
	}
	return l, nil
}

func (q *Service) GetVisibleJobLength(queueName string) (int, error) {
	attr := "ApproximateNumberOfMessages"
	return q.getIntAttribute(queueName, attr)
}

func (q *Service) GetNotVisibleJobLength(queueName string) (int, error) {
	attr := "ApproximateNumberOfMessagesNotVisible"
	return q.getIntAttribute(queueName, attr)
}
