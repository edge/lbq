package lbq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edge/atomiccounter"
)

var ErrNoResults = errors.New("There are no scored items")

type Manager struct {
	AverageResponseTime *atomiccounter.Counter
	RequestCount        *atomiccounter.Counter

	devices       sync.Map // [string]*DeviceScore
	priorityQueue *PriorityQueue
}

type DeviceScore struct {
	pendingRequestsCount uint64 // change to *atomiccounter.Counter
	handledRequestsCount uint64 // change to *atomiccounter.Counter
	averageResponse      uint64 // change to *atomiccounter.Counter
}

// Score returns the device score
func (d *DeviceScore) Score() int {
	return int(d.averageResponse * (d.pendingRequestsCount + 1))
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
func (ism *Manager) Next() (string, int, error) {
	if ism.priorityQueue.Len() == 0 {
		return "", 0, ErrNoResults
	}
	i := ism.priorityQueue.Peek().(*Item)
	return i.ID, i.Priority, nil
}

// AddClientWithContext adds the client to the device store, with a context.
func (ism *Manager) AddClientWithContext(ctx context.Context, key string) bool {
	fmt.Printf("scoreManager: adding host %q\n", key)

	if _, existantDevice := ism.devices.Load(key); existantDevice {
		fmt.Printf("scoreManager: client already exists: %q\n", key)
		return !existantDevice
	}

	device := &DeviceScore{
		handledRequestsCount: 0,
		averageResponse:      ism.avgResponseTime(),
		pendingRequestsCount: 0,
	}
	ism.devices.Store(key, device)

	item := &Item{
		Priority: device.Score(),
		ID:       key,
	}

	ism.priorityQueue.Push(item)
	return true
}

// AddClient add a new host.
func (ism *Manager) AddClient(key string) bool {
	return ism.AddClientWithContext(context.Background(), key)
}

// RemoveClient removes the client from the score engine.
func (ism *Manager) RemoveClient(key string) {
	ism.devices.Delete(key)
}

// ClientStartJob tells the manager that a new request is sent to device `id`
func (ism *Manager) ClientStartJob(id string) {
	var device *DeviceScore

	d, existantDevice := ism.devices.Load(id)
	if !existantDevice {
		device = &DeviceScore{
			handledRequestsCount: 1,
			averageResponse:      ism.avgResponseTime(),
			pendingRequestsCount: 1,
		}
		ism.devices.Store(id, device)
	} else {
		device = d.(*DeviceScore)
	}

	// increment pendingRequestsCount.
	atomic.AddUint64(&device.pendingRequestsCount, 1)

	item := &Item{
		Priority: device.Score(),
		ID:       id,
	}

	// Update the heap structure
	ism.priorityQueue.Push(item)
}

// ClientEndJob tells the manager that device `id` finished handling a request
func (ism *Manager) ClientEndJob(id string, canceled bool, responseTime time.Duration) {
	var device *DeviceScore

	d, existantDevice := ism.devices.Load(id)

	if !existantDevice {
		// If this is a canceled job and the device doesn't exist
		// there is no need to proceed with scoring.
		if canceled {
			return
		}
		device = &DeviceScore{
			handledRequestsCount: 0,
			averageResponse:      ism.avgResponseTime(),
			pendingRequestsCount: 0,
		}
		ism.devices.Store(id, device)
	} else {
		device = d.(*DeviceScore)
	}

	// decrement pendingRequestsCount. panic if it becomes negative, since this should NEVER happen.
	if atomic.AddUint64(&device.pendingRequestsCount, ^uint64(0)) < 0 {
		panic("negative pendingRequestsCount")
	}

	// update average response time and increment handled count, unless this is a canceled request.
	if !canceled {
		device.averageResponse = ((device.averageResponse * device.handledRequestsCount) + uint64(responseTime)) / (device.handledRequestsCount + 1)
		atomic.AddUint64(&device.handledRequestsCount, 1)
	}

	item := &Item{
		Priority: device.Score(),
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
		v := value.(*DeviceScore)
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
		AverageResponseTime: atomiccounter.New(),
		RequestCount:        atomiccounter.New(),
		priorityQueue:       pq,
	}

	return ism
}
