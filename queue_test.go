package sqsq_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/maruware/sqsq"
)

func buildAwsConfig() *aws.Config {
	config := aws.NewConfig()
	config.WithRegion("us-east-1")
	endpoint := os.Getenv("SQS_ENDPOINT")
	config.WithEndpoint(endpoint)

	return config
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
	q, err := sqsq.NewQueue(a, &sqsq.Config{Debug: true})
	if err != nil {
		t.Fatalf("failed to construct queue: %v", err)
	}

	name := os.Getenv("SQS_QUEUE")
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
