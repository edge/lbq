package lbq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ScoreEngine represents a client <> job scoring engine.
type ScoreEngine interface {
	// Next returns the key for a client and its current score.
	Next() (device *Device, score int, err error)
	// WaitForClients waits if there are not enough clients.
	WaitForClients(ctx context.Context) bool
	// AddClient adds a new client and returns false if the insert failed.
	AddClient(key string) *Device
	// AddClientWithContext inserts a client into the score engine with a context.
	AddClientWithContext(ctx context.Context, key string) *Device
	// RemoveClient removes a client by key.
	RemoveClient(key string)
	// ClientStartJob tells the score manager that a client has started a job.
	ClientStartJob(key string)
	// ClientEndJob tells the score engine that a job has been completed. Used for both canceled and successful jobs.
	ClientEndJob(key string, canceled bool, timeTaken time.Duration)
	// Reset clears all score and client data.
	Reset()
	// Dump dumps the current status to stdout.
	Dump()
}

var (
	defaultLoadBalancer = NewImpactScore()

	// ErrSetEngineAfterStart is thrown when an attempt to set an engine is made following a queue start.
	ErrSetEngineAfterStart = errors.New("ScoreEngine can't be changed after Start has been called")
)

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
				// This job has closed.
				if j.ctx.Err() != nil {
					continue
				}
				go q.deviceJob(j)
			}
		}
	}()
}

func (q *Queue) deviceJob(j *job) {
	device, _, err := q.ScoreEngine.Next()
	fmt.Println("Device", device.ID)
	if err != nil {
		fmt.Println("NO DEVICE: ADD BACK TO QUEUE")
		time.Sleep(10 * time.Millisecond)
		q.jobs <- j
		return
	}
	if err := device.DoJob(j.payload); err != nil {
		fmt.Println("DEVICE DO ERROR: INSERT IT AGAIN")
		time.Sleep(10 * time.Millisecond)
		q.jobs <- j
		return
	}
}

func (q *Queue) setDefaults() {
	if q.ScoreEngine == nil {
		q.ScoreEngine = defaultLoadBalancer
	}
}

// Do runs a job.
func (q *Queue) Do(j interface{}, ctx context.Context) {
	if ok := q.ScoreEngine.WaitForClients(ctx); !ok {
		return
	}

	q.jobs <- &job{
		payload: j,
		ctx:     ctx,
	}
}

// StartWithContext starts the queue service with a supplied context.
func (q *Queue) StartWithContext(ctx context.Context) {
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

// Start launches the queue service via StartWithContext.
func (q *Queue) Start() {
	q.StartWithContext(context.Background())
}

// New creates a new instance of queue.
func New() *Queue {
	return &Queue{
		jobs: make(chan *job, 0),
	}
}
