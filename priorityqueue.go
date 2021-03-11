package lbq

import (
	"container/heap"
	"sync"
)

// An Item is something we manage in a priority queue.
type Item struct {
	ID       string
	Priority int
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	sync.RWMutex
	PriorityQueueItems
	quitChan       chan bool
	heapPushChan   chan heapPushScoreChan
	heapPopChan    chan heapPopScoreChan
	heapPeekChan   chan heapPopScoreChan
	heapRemoveChan chan heapRemoveScoreChan
}

// A PriorityQueueItems implements heap.Interface and holds Items.
type PriorityQueueItems []*Item

type heapPushScoreChan struct {
	item interface{}
}

type heapPopScoreChan struct {
	result interface{}
}

type heapRemoveScoreChan struct {
	id interface{}
}

func (pq PriorityQueueItems) Len() int {
	return len(pq)
}

func (pq PriorityQueueItems) Less(i, j int) bool {
	return pq[j].Priority > pq[i].Priority
}

func (pq PriorityQueueItems) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Remove removes an item from the queue.
func (pq *PriorityQueue) Remove(id string) {
	pq.heapRemoveChan <- heapRemoveScoreChan{id}
}

func (pq *PriorityQueue) remove(x interface{}) {
	pq.Lock()
	defer pq.Unlock()
	id := x.(string)
	var found *Item

	for _, v := range pq.PriorityQueueItems {
		if v.ID == id {
			found = v
			break
		}
	}

	if found == nil {
		return
	}

	pq.PriorityQueueItems = pq.PriorityQueueItems[:found.index+copy(pq.PriorityQueueItems[found.index:], pq.PriorityQueueItems[found.index+1:])]

	// Reset indexes
	for _, v := range pq.PriorityQueueItems {
		if v.index > found.index {
			v.index--
		}
	}
	heap.Init(pq)
}

// Push adds an item to the heap.
func (pq *PriorityQueue) Push(item interface{}) {
	pq.heapPushChan <- heapPushScoreChan{item}
}

func (pq *PriorityQueue) push(x interface{}) {
	pq.Lock()
	defer pq.Unlock()
	item := x.(*Item)

	// Overwrite priority and index if the entry exists.
	for _, v := range pq.PriorityQueueItems {
		if v.ID == item.ID {
			v.Priority = item.Priority
			heap.Fix(pq, v.index)
			return
		}
	}

	item.index = 0
	// Increase indexes
	for _, v := range pq.PriorityQueueItems {
		v.index++
	}
	pq.PriorityQueueItems = append(PriorityQueueItems{item}, pq.PriorityQueueItems...)
}

// Peek returns the top of the list item without popping it.
func (pq *PriorityQueue) Peek() interface{} {
	var result = make(chan interface{})
	pq.heapPeekChan <- heapPopScoreChan{
		result: result,
	}
	return <-result
}

func (pq *PriorityQueue) peek() interface{} {
	old := pq.PriorityQueueItems
	item := old[0]
	return item
}

func (pq *PriorityQueue) Pop() interface{} {
	var result = make(chan interface{})
	pq.heapPopChan <- heapPopScoreChan{
		result: result,
	}
	return <-result
}

func (pq *PriorityQueue) pop() interface{} {
	old := pq.PriorityQueueItems
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.PriorityQueueItems = old[0 : n-1]
	return item
}

// NewPriorityQueue returns a new PriorityQueue
func NewPriorityQueue() *PriorityQueue {
	pq := PriorityQueue{
		PriorityQueueItems: make(PriorityQueueItems, 0),
		heapPushChan:       make(chan heapPushScoreChan),
		heapPopChan:        make(chan heapPopScoreChan),
		heapPeekChan:       make(chan heapPopScoreChan),
		heapRemoveChan:     make(chan heapRemoveScoreChan),
	}
	heap.Init(&pq)
	pq.quitChan = pq.watchHeapOps()
	return &pq
}

// StopWatchHeapOps stops watching for priorityQueue operations
func (pq *PriorityQueue) StopWatchHeapOps() {
	pq.quitChan <- true
}

// watchHeapOps watch for operations to our priorityQueue, and serializing the operations with channels
func (pq *PriorityQueue) watchHeapOps() chan bool {
	var quit = make(chan bool)
	go func() {
		for {
			select {
			case <-quit:
				return
			case pushScore := <-pq.heapPushChan:
				pq.push(pushScore.item)
			case popScore := <-pq.heapPopChan:
				popScore.result.(chan interface{}) <- pq.pop()
			case peekScore := <-pq.heapPeekChan:
				peekScore.result.(chan interface{}) <- pq.peek()
			case removeScore := <-pq.heapRemoveChan:
				pq.remove(removeScore.id)
			}

		}
	}()
	return quit
}
