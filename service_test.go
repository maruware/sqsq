package sqsq_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/maruware/sqsq"
)

func buildAwsConfig() *aws.Config {
	config := aws.NewConfig()
	config.WithRegion("us-east-1")
	endpoint := os.Getenv("SQS_ENDPOINT")
	config.WithEndpoint(endpoint)

	return config
}

func setupQueue(config *aws.Config, name string) {
	sess := session.New(config)
	s := sqs.New(sess)
	s.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
}

func Contains(s []string, t string) bool {
	for _, e := range s {
		if e == t {
			return true
		}
	}
	return false
}

func TestScenario(t *testing.T) {
	a := buildAwsConfig()
	q, err := sqsq.New(a, &sqsq.Config{Debug: true})
	if err != nil {
		t.Fatalf("failed to construct queue: %v", err)
	}

	name := "my-queue"
	setupQueue(a, name)

	if err := q.UseQueue(name); err != nil {
		t.Fatalf("faild use queue: %v", err)
	}

	jobChan := make(chan *sqsq.Job, 2)

	ctx, cancel := context.WithCancel(context.Background())
	go q.WatchQueue(ctx, name, 2, 5, 5, jobChan)

	msgs := make([]string, 2)
	wg := &sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			job := <-jobChan
			defer job.Release()

			d := job.GetData()
			msgs[i] = *d
			job.Done()

			wg.Done()
		}(i)
	}

	body1 := "test-job1"
	if err := q.PutJob(name, body1, 0); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}
	body2 := "test-job2"
	if err := q.PutJob(name, body2, 0); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}

	wg.Wait()

	if !Contains(msgs, body1) {
		t.Errorf("expect to receive job message[%s].", body1)
	}
	if !Contains(msgs, body2) {
		t.Errorf("expect to receive job message[%s].", body2)
	}

	cancel()
}

func TestPostpone(t *testing.T) {
	a := buildAwsConfig()
	q, err := sqsq.New(a, &sqsq.Config{Debug: true})
	if err != nil {
		t.Fatalf("failed to construct queue: %v", err)
	}

	name := "postpone-test"
	setupQueue(a, name)

	if err := q.UseQueue(name); err != nil {
		t.Fatalf("faild use queue: %v", err)
	}

	jobChan := make(chan *sqsq.Job, 1)

	ctx, cancel := context.WithCancel(context.Background())
	go q.WatchQueue(ctx, name, 1, 5, 5, jobChan)

	msgChan := make(chan *string)
	go func() {
		job := <-jobChan
		defer job.Release()

		msgChan <- job.GetData()
		err := job.Postpone(time.Second * 60)
		if err != nil {
			t.Error("failed to postpone")
		}
	}()

	body := "test-job"
	if err := q.PutJob(name, body, 0); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}

	msg := <-msgChan

	if *msg != body {
		t.Errorf("expect to receive job message[%s].", body)
	}

	cancel()

	visibleLen, err := q.GetVisibleJobLength(name)
	if err != nil {
		t.Fatalf("failed to get visible job length")
	}

	if visibleLen != 0 {
		t.Errorf("expect empty visible messaage if job was postponed. len = %d", visibleLen)
	}

	notVisibleLen, err := q.GetNotVisibleJobLength(name)
	if err != nil {
		t.Fatalf("failed to get not visible job length")
	}

	if notVisibleLen != 1 {
		t.Errorf("expect 1 visible messaage if job was postponed. len = %d", notVisibleLen)
	}

}
