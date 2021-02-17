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
	quitChan       chan bool
	heapPushChan   chan heapPushScoreChan
	heapPopChan    chan heapPopScoreChan
	heapPeekChan   chan heapPopScoreChan
	heapRemoveChan chan heapRemoveScoreChan
	items          PriorityQueueItems
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

func (pq PriorityQueue) Len() int {
	size := len(pq.items)
	return size
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.items[i].Priority > pq.items[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
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

	for _, v := range pq.items {
		if v.ID == id {
			found = v
			break
		}
	}

	// Remove found item
	pq.items = pq.items[:found.index+copy(pq.items[found.index:], pq.items[found.index+1:])]

	// Reset indexes
	for _, v := range pq.items {
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
	exists := false

	// Overwrite priority and index if the entry exists.
	for _, v := range pq.items {
		if v.ID == item.ID {
			v.Priority = item.Priority
			item.index = v.index
			exists = true
			break
		}
	}

	if !exists {
		n := pq.Len()
		item.index = n
		pq.items = append(pq.items, item)
	}

	heap.Fix(pq, item.index)
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
	old := *pq
	n := len(old.items)
	item := old.items[n-1]
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
	old := *pq
	n := len(old.items)
	item := old.items[n-1]
	old.items[n-1] = nil // avoid memory leak
	item.index = -1      // for safety
	pq.items = old.items[0 : n-1]
	return item
}

// NewPriorityQueue returns a new PriorityQueue
func NewPriorityQueue() *PriorityQueue {
	pq := PriorityQueue{
		items:          make(PriorityQueueItems, 0),
		heapPushChan:   make(chan heapPushScoreChan),
		heapPopChan:    make(chan heapPopScoreChan),
		heapPeekChan:   make(chan heapPopScoreChan),
		heapRemoveChan: make(chan heapRemoveScoreChan),
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
