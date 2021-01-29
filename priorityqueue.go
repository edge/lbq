package lbq

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	ID       string
	Priority int
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

type heapPushScoreChan struct {
	score    interface{}
	priority *int
}

type heapPopScoreChan struct {
	result interface{}
}

var (
	quitChan chan bool
	// heapPushChan - push channel for pushing to a heap
	heapPushChan = make(chan heapPushScoreChan)
	// heapPopChan - pop channel for popping from a heap
	heapPopChan = make(chan heapPopScoreChan)
	// heapPeekChan - peek channel for peeking from a heap
	heapPeekChan = make(chan heapPopScoreChan)
)

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[j].Priority < pq[i].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	heapPushChan <- heapPushScoreChan{
		score: x,
	}
}

func (pq *PriorityQueue) push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
	heap.Fix(pq, item.index)
}

// Peek returns the top of the list item without popping it.
func (pq *PriorityQueue) Peek() interface{} {
	var result = make(chan interface{})
	heapPeekChan <- heapPopScoreChan{
		result: result,
	}
	return <-result
}

func (pq *PriorityQueue) peek() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	return item
}

func (pq *PriorityQueue) Pop() interface{} {
	var result = make(chan interface{})
	heapPopChan <- heapPopScoreChan{
		result: result,
	}
	return <-result
}

func (pq *PriorityQueue) pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Update safely updates an item from the priorityQueue
func (pq *PriorityQueue) Update(item *Item, priority int) {
	heapPushChan <- heapPushScoreChan{
		score:    item,
		priority: &priority,
	}
}

// update modifies the priority of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, priority int) {
	item.Priority = priority
	heap.Fix(pq, item.index)
}

// PriorityQueue returns a new PriorityQueue
func NewPriorityQueue() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	quitChan = pq.watchHeapOps()
	return &pq
}

// StopWatchHeapOps stops watching for priorityQueue operations
func StopWatchHeapOps() {
	quitChan <- true
}

// watchHeapOps watch for operations to our priorityQueue, and serializing the operations with channels
func (pq *PriorityQueue) watchHeapOps() chan bool {
	var quit = make(chan bool)
	go func() {
		for {
			select {
			case <-quit:
				return
			case pushScore := <-heapPushChan:
				if pushScore.priority != nil {
					pq.update((pushScore.score).(*Item), *(pushScore.priority))
				} else {
					pq.push(pushScore.score)
				}
			case popScore := <-heapPopChan:
				popScore.result.(chan interface{}) <- pq.pop()
			case peekScore := <-heapPeekChan:
				peekScore.result.(chan interface{}) <- pq.peek()
			}

		}
	}()
	return quit
}
