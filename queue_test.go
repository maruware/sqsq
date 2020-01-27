package sqsq_test

import (
	"context"
	"os"
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

	ctx, cancel := context.WithCancel(context.Background())

	jobChan := make(chan *sqsq.Job)

	go q.WatchQueue(ctx, name, 1, 5, 5, jobChan)

	body := "test-job"
	q.PutJob(name, body, 0)

	job := <-jobChan
	d := job.GetData()

	if *d != body {
		t.Errorf("mismatch job message body. expect = %s, actual = %s", body, *d)
	}

	cancel()
}
