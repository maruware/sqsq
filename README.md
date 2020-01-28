# sqsq

[![Actions Status](https://github.com/maruware/sqsq/workflows/Test/badge.svg)](https://github.com/maruware/sqsq/actions)

Job Queue for Go based on SQS 


## Usage

### worker

```go

import (
    "github.com/maruware/sqsq"
    "github.com/aws/aws-sdk-go/aws"
)

func startWorker(queueName string) {
    config := aws.NewConfig()
    q, err := sqsq.New(config)

    err := q.UseQueue(queueName)
    jobChan := make(chan *sqsq.Job, 2)

    var concurrency int64 = 2
    var visibilityTimeout int64 = 5
    var waitTimeSeconds int64 = 20
    go q.WatchQueue(ctx, queueName, concurrency, visibilityTimeout, waitTimeSeconds, jobChan)

    for {
        select {
        case job := <-jobChan:
            go func () {
                defer job.Release()
                data := job.GetData()

                // do something

                job.Done()
            }()
        case <-ctx.Done():
            return
        }
    }
}

```

### publisher

```go
import (
    "github.com/maruware/sqsq"
    "github.com/aws/aws-sdk-go/aws"
)

func someProcess(queueName string) {
    config := aws.NewConfig()
    q, err := sqsq.New(config)

    // do something

    config := aws.NewConfig()
    q, err := sqsq.New(config)
    err := q.PutJob(queueName, "sample message", 0)
}
```
