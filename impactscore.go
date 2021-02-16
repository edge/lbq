package lbq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/edge/atomiccounter"
	"github.com/edge/atomicstore"
)

var (
	ErrDeviceMissing = errors.New("Unable to find device")
	ErrNoResults     = errors.New("There are no scored items")
)

type Manager struct {
	AverageResponseTime *atomiccounter.Counter
	RequestCount        *atomiccounter.Counter

	devices       *atomicstore.Store
	priorityQueue *PriorityQueue
}

type Device struct {
	ID                   string
	jobChan              chan interface{}
	pendingRequestsCount uint64 // change to *atomiccounter.Counter
	handledRequestsCount uint64 // change to *atomiccounter.Counter
	averageResponse      uint64 // change to *atomiccounter.Counter
}

// Score returns the device score
func (d *Device) Score() int {
	return int(d.averageResponse * (d.pendingRequestsCount + 1))
}

func (d *Device) DoJob(r interface{}) {
	d.jobChan <- r
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

// Next returns the next Host that should handle a request
func (ism *Manager) Next() (*Device, int, error) {
	if ism.priorityQueue.Len() == 0 {
		return nil, 0, ErrNoResults
	}
	i := ism.priorityQueue.Peek().(*Item)
	if d, exists := ism.devices.Get(i.ID); exists {
		return d.(*Device), i.Priority, nil
	}
	return nil, 0, ErrDeviceMissing
}

func (ism *Manager) newDevice(id string, count uint64) *Device {
	return &Device{
		ID:                   id,
		jobChan:              make(chan interface{}, 0),
		handledRequestsCount: count,
		averageResponse:      ism.avgResponseTime(),
		pendingRequestsCount: count,
	}
}

// removeOnContextClose removes the device when a context is closed.
func (ism *Manager) removeOnContextClose(ctx context.Context, device *Device) {
	<-ctx.Done()
	close(device.jobChan)
	ism.RemoveClient(device.ID)
}

// AddClientWithContext adds the client to the device store, with a context.
func (ism *Manager) AddClientWithContext(ctx context.Context, key string) chan interface{} {
	fmt.Printf("scoreManager: adding host %q\n", key)

	device := ism.newDevice(key, 0)

	if _, existantDevice := ism.devices.Upsert(key, device); existantDevice {
		fmt.Printf("scoreManager: client already exists: %q\n", key)
		return nil
	}

	item := &Item{
		Priority: device.Score(),
		ID:       key,
	}

	ism.priorityQueue.Push(item)
	go ism.removeOnContextClose(ctx, device)
	return device.jobChan
}

// AddClient add a new host.
func (ism *Manager) AddClient(key string) chan interface{} {
	return ism.AddClientWithContext(context.Background(), key)
}

// RemoveClient removes the client from the score engine.
func (ism *Manager) RemoveClient(key string) {
	ism.devices.Delete(key)
	ism.priorityQueue.Remove(key)
}

// ClientStartJob tells the manager that a new request is sent to device `id`
func (ism *Manager) ClientStartJob(id string) {
	device := ism.newDevice(id, 1)
	found, _ := ism.devices.Upsert(id, device)
	d := found.(*Device)

	// increment pendingRequestsCount.
	atomic.AddUint64(&d.pendingRequestsCount, 1)

	item := &Item{
		Priority: d.Score(),
		ID:       id,
	}

	// Update the heap structure
	ism.priorityQueue.Push(item)
}

// ClientEndJob tells the manager that device `id` finished handling a request
func (ism *Manager) ClientEndJob(id string, canceled bool, responseTime time.Duration) {
	device := ism.newDevice(id, 1)
	found, _ := ism.devices.Upsert(id, device)
	d := found.(*Device)

	// decrement pendingRequestsCount. panic if it becomes negative, since this should NEVER happen.
	if atomic.AddUint64(&d.pendingRequestsCount, ^uint64(0)) < 0 {
		panic("negative pendingRequestsCount")
	}

	// update average response time and increment handled count, unless this is a canceled request.
	if !canceled {
		d.averageResponse = ((d.averageResponse * d.handledRequestsCount) + uint64(responseTime)) / (d.handledRequestsCount + 1)
		atomic.AddUint64(&d.handledRequestsCount, 1)
	}

	item := &Item{
		Priority: d.Score(),
		ID:       id,
	}

	if !canceled {
		ism.addRequest(responseTime)
	}

	// Update the heap structure
	ism.priorityQueue.Push(item)
}

// Dump dumps the current manager status to stdout
func (ism *Manager) Dump() {
	ism.devices.Range(func(key, value interface{}) bool {
		d := key.(string)
		v := value.(*Device)
		fmt.Printf("device %s | avg response %d | pending %d | score %d\n", d, v.averageResponse, v.pendingRequestsCount, v.Score())
		return true
	})
}

// Reset clears all score and client data.
func (ism *Manager) Reset() {
	// TODO: clear and reset score manager
}

func (ism *Manager) avgResponseTime() uint64 {
	return ism.AverageResponseTime.Get()
}

// NewImpactScore creates a new impact score manager
func NewImpactScore() ScoreEngine {
	pq := NewPriorityQueue()
	ism := &Manager{
		devices:             atomicstore.New(false),
		AverageResponseTime: atomiccounter.New(),
		RequestCount:        atomiccounter.New(),
		priorityQueue:       pq,
	}

	return ism
}
