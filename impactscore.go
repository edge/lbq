package lbq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/edge/atomiccounter"
	"github.com/edge/workers"
)

var (
	ErrDeviceMissing = errors.New("Unable to find device")
	ErrNoResults     = errors.New("There are no scored items")
)

type Manager struct {
	*workers.Pool
	AverageResponseTime *atomiccounter.Counter
	RequestCount        *atomiccounter.Counter
	priorityQueue       *PriorityQueue
}

func (ism *Manager) avgResponseTime() uint64 {
	return ism.AverageResponseTime.Get()
}

// addRequest updates the global manager metrics.
func (ism *Manager) addRequest(duration time.Duration) {
	d := uint64(duration)
	for {
		count := ism.RequestCount.Get()
		oldV := ism.AverageResponseTime.Get()
		newV := uint64(math.Round(float64(oldV*count+d) / float64(count+1)))
		// ism.AverageResponseTime.Set(newV)
		if atomic.CompareAndSwapUint64((*uint64)(ism.AverageResponseTime), oldV, newV) {
			break
		}
	}

	ism.RequestCount.Inc()
}

func (ism *Manager) newDevice(id string, count uint64) workers.Worker {
	w := workers.NewDefaultWorker(id)
	w.SetMetadata(&deviceMetadata{
		handledRequestsCount: count,
		averageResponse:      ism.avgResponseTime(),
		pendingRequestsCount: count,
	})
	return w
}

// Next returns the next Host that should handle a request
func (ism *Manager) Next() (workers.Worker, int, error) {
	if ism.priorityQueue.Len() == 0 {
		return nil, 0, ErrNoResults
	}
	i := ism.priorityQueue.Peek().(*Item)
	if d, exists := ism.Get(i.ID); exists {
		return d.(workers.Worker), i.Priority, nil
	}
	return nil, 0, ErrDeviceMissing
}

func (ism *Manager) getDeviceMeta(d workers.Worker) *deviceMetadata {
	md := d.GetMetadata()
	if metadata, ok := md.(*deviceMetadata); ok {
		return metadata
	}

	return nil
}

func (ism *Manager) getDeviceScore(d workers.Worker) int {
	if md := ism.getDeviceMeta(d); md != nil {
		return int(md.averageResponse * (md.pendingRequestsCount + 1))
	}
	return 0
}

func (ism *Manager) incDevicePending(d workers.Worker) {
	if md := ism.getDeviceMeta(d); md != nil {
		atomic.AddUint64(&md.pendingRequestsCount, 1)
	}
}

func (ism *Manager) incDeviceHandled(d workers.Worker) {
	if md := ism.getDeviceMeta(d); md != nil {
		atomic.AddUint64(&md.handledRequestsCount, 1)
	}
}

func (ism *Manager) decDevicePending(d workers.Worker) {
	if md := ism.getDeviceMeta(d); md != nil {
		// decrement pendingRequestsCount. panic if it becomes negative, since this should NEVER happen.
		if atomic.AddUint64(&md.pendingRequestsCount, ^uint64(0)) < 0 {
			panic("negative pendingRequestsCount")
		}
	}
}

func (ism *Manager) setDeviceAvgResponse(d workers.Worker, responseTime time.Duration) {
	if md := ism.getDeviceMeta(d); md != nil {
		md.averageResponse = ((md.averageResponse * md.handledRequestsCount) + uint64(responseTime)) / (md.handledRequestsCount + 1)
	}
}

// AddClientWithContext adds the client to the device store, with a context.
func (ism *Manager) AddClientWithContext(ctx context.Context, key string) workers.Worker {
	fmt.Printf("scoreManager: adding host %q\n", key)

	device := ism.newDevice(key, 0)

	if d, err := ism.Add(ctx, device); err != nil {
		fmt.Errorf("scoreManager failed to insert %s: %w\n", key, err)
		return d.(workers.Worker)
	}

	item := &Item{
		Priority: ism.getDeviceScore(device),
		ID:       key,
	}

	ism.priorityQueue.Push(item)

	return device
}

// AddClient add a new host.
func (ism *Manager) AddClient(key string) workers.Worker {
	return ism.AddClientWithContext(context.Background(), key)
}

// RemoveClient removes the client from the score engine.
func (ism *Manager) RemoveClient(key string) {
	fmt.Printf("scoreManager: remove host %q\n", key)
	ism.Remove(key)
	ism.priorityQueue.Remove(key)
}

// ClientStartJob tells the manager that a new request is sent to device `id`
func (ism *Manager) ClientStartJob(id string) {
	found, exists := ism.Get(id)

	if !exists {
		return
	}

	d := found.(workers.Worker)

	// increment pendingRequestsCount.
	ism.incDevicePending(d)

	item := &Item{
		Priority: ism.getDeviceScore(d),
		ID:       id,
	}

	// Update the heap structure
	ism.priorityQueue.Push(item)
}

// ClientEndJob tells the manager that device `id` finished handling a request
func (ism *Manager) ClientEndJob(id string, canceled bool, responseTime time.Duration) {
	found, exists := ism.Get(id)

	if !exists {
		return
	}
	d := found.(workers.Worker)

	ism.decDevicePending(d)

	// update average response time and increment handled count, unless this is a canceled request.
	if !canceled {
		ism.setDeviceAvgResponse(d, responseTime)
		ism.incDeviceHandled(d)
	}

	item := &Item{
		Priority: ism.getDeviceScore(d),
		ID:       id,
	}

	if !canceled {
		ism.addRequest(responseTime)
	}

	// Update the heap structure
	ism.priorityQueue.Push(item)
}

// GetDevice gets a device by ID.
func (ism *Manager) GetDevice(key string) (interface{}, bool) {
	return ism.Get(key)
}

// Dump dumps the current manager status to stdout
func (ism *Manager) Dump() {
	ism.Range(func(key, value interface{}) bool {
		k := key.(string)
		d := value.(workers.Worker)
		if md := ism.getDeviceMeta(d); md != nil {
			fmt.Printf("device %s | avg response %d | pending %d | score %d\n", k, md.averageResponse, md.pendingRequestsCount, ism.getDeviceScore(d))
		}
		return true
	})
}

// Reset clears all score and client data.
func (ism *Manager) Reset() {
	// TODO: clear and reset score manager
}

// NewImpactScore creates a new impact score manager
func NewImpactScore() ScoreEngine {
	pq := NewPriorityQueue()
	ism := &Manager{
		Pool:                workers.New(),
		AverageResponseTime: atomiccounter.New(),
		RequestCount:        atomiccounter.New(),
		priorityQueue:       pq,
	}

	return ism
}
