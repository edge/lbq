package lbq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/edge/workers"
)

// ScoreEngine represents a client <> job scoring engine.
type ScoreEngine interface {
	// Next returns the key for a client and its current score.
	Next() (device workers.Worker, score int, err error)
	// AddClient adds a new client and returns false if the insert failed.
	AddClient(key string) workers.Worker
	// RemoveClient remove a client by key
	RemoveClient(key string)
	// AddClientWithContext inserts a client into the score engine with a context.
	AddClientWithContext(ctx context.Context, key string) workers.Worker
	// ClientStartJob tells the score manager that a client has started a job.
	ClientStartJob(key string)
	// ClientEndJob tells the score engine that a job has been completed. Used for both canceled and successful jobs.
	ClientEndJob(key string, canceled bool, timeTaken time.Duration)
	// GetDevice finds a device by key
	GetDevice(key string) (interface{}, bool)
	// Reset clears all score and client data.
	Reset()
	// Dump dumps the current status to stdout.
	Dump()
}

const throttle time.Duration = time.Duration(time.Second)

// ErrSetEngineAfterStart is thrown when an attempt to set an engine is made following a queue start.
var ErrSetEngineAfterStart = errors.New("ScoreEngine can't be changed after Start has been called")

// Queue is a atomic data store with a load balancer.
type Queue struct {
	jobs chan *job
	ScoreEngine
}

type job struct {
	payload interface{}
	ctx     context.Context
}

func (q *Queue) server(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case j := <-q.jobs:
				go q.sendToNextWorker(j)
			}
		}
	}()
}

func (q *Queue) retryJob(j *job) {
	// Wait for a short time before adding back to the queue.
	select {
	case <-j.ctx.Done():
		return
	case <-time.After(throttle):
		// Attempt to add the job back.
		select {
		case q.jobs <- j:
			return
		default:
			fmt.Printf("Failed to insert job on retry")
			return
		}
	}
}

// sendToNextWorker finds the next worker and sends it a job.
func (q *Queue) sendToNextWorker(j *job) {
	// This job has closed.
	if j.ctx.Err() != nil {
		return
	}
	worker, _, err := q.ScoreEngine.Next()
	if err == nil {
		q.sendToWorker(worker, j)
		return
	}

	fmt.Printf("NO DEVICE: ADD BACK TO QUEUE %v\n", err)
	go q.retryJob(j)
}

// sendToWorker sends the job to a worker.
func (q *Queue) sendToWorker(d workers.Worker, j *job) {
	// This job has closed.
	if j.ctx.Err() != nil {
		return
	}
	if err := d.AddJob(j.payload); err != nil {
		fmt.Printf("DEVICE DO ERROR: INSERT IT AGAIN %v\n", err)
		go q.retryJob(j)
	}
}

func (q *Queue) setDefaults() {
	if q.ScoreEngine == nil {
		q.ScoreEngine = NewImpactScore()
	}
}

// Do runs a job.
func (q *Queue) Do(ctx context.Context, payload interface{}, workerID string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	workerJob := &job{
		payload: payload,
		ctx:     ctx,
	}
	// If a device is specified bypass the job queue and send directly.
	if workerID != "" {
		if d, ok := q.ScoreEngine.GetDevice(workerID); ok {
			worker := d.(workers.Worker)
			go q.sendToWorker(worker, workerJob)
			return nil
		}
	}
	q.jobs <- workerJob

	return nil
}

// Start starts the queue service with a supplied context.
func (q *Queue) Start(ctx context.Context) {
	q.setDefaults()
	q.server(ctx)
}

// WithEngine sets the score engine for the queue.
func (q *Queue) WithEngine(engine ScoreEngine) error {
	if q.ScoreEngine != nil {
		return ErrSetEngineAfterStart
	}

	q.ScoreEngine = engine
	return nil
}

// New creates a new instance of queue.
func New() *Queue {
	return &Queue{
		jobs: make(chan *job),
	}
}
