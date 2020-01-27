# sqsq
Job Queue for Go based on SQS 

## Usage

* worker

```go
config := aws.NewConfig()
q, err := sqsq.NewQueue(config)

err := q.UseQueue("my-queue")
jobChan := make(chan *sqsq.Job, 2)

go q.WatchQueue(ctx, name, 2, 5, 5, jobChan)

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
```

* publisher

```go

config := aws.NewConfig()
q, err := sqsq.NewQueue(config)
err := q.PutJob("my-queue", "sample message", 0)
```
