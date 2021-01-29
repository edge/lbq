package lbq

import (
	"context"

	"github.com/edge/gateway/pkg/impactscore"
	"github.com/edge/atomicstore"
)

type ScoreEngine interface {
	// Next returns the key for a client and its current score.
	func Next() key string, score int
	// AddClient adds a new client and returns false if the insert failed.
	func AddClient(key string) bool
	// RemoveClient removes a client by key.
	func RemoveClient(key string)
	// AddClientWithContext inserts a client into the score engine with a context.
	func AddClientWithContext(ctx context.Context, key string)
	// ClientStartJob tells the score manager that a client has started one or more jobs.
	func ClientStartJob(key string, count int)
	// ClientEndJob tells the score engine that a job has been completed. Used for both canceled and successful jobs.
	func ClientEndJob(key string, canceled bool, timeTaken time.Duration)
	// Reset clears all score and client data.
	func Reset()
	// Dump dumps the current status to stdout.
	func Dump()
}

// TODO: Define edge/impactscore as default load balancer.
var defaultLoadBalancer = impactscore.New()

const (
	ERR_SET_ENGINE_AFTER_START = errors.New("ScoreEngine can't be changed after Start has been called")
)

type Queue struct {
	jobs *atomicstore.Store
	scoreEngine *ScoreEngine
}

func (q *Queue) server(ctx context.Context) {
	// TODO: watch jobs for change
	// TODO: Get device with q.lb.Next()
	// TODO: Send request to devices request chan
}

func (q *Queue) setDefaults() {
	if q.lb == nil {
		q.lb = defaultLoadBalancer
	}
}

// StartWithContext starts the queue service with a supplied context.
func (q *Queue) StartWithContext(ctx context.Context) {
	q.setDefaults()
	q.server(ctx)
}

// WithEngine sets the score engine for the queue.
func (q *Queue) WithEngine(engine ScoreEngine) error {
	if q.scoreEngine != nil {
		return ERR_SET_ENGINE_AFTER_START
	}
	
	q.scoreEngine = engine
	return nil
}

// Start launches the queue service via StartWithContext.
func (q *Queue) Start() {
	q.StartWithContext(context.Background())
}

// New creates a new instance of queue.
func New() *Queue {
	return &Queue{
		*atomicstore.New(true),
	}
}
