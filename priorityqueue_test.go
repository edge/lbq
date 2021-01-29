package lbq

import (
	"fmt"
	"testing"
)

// func TestImpactScoreManager_NewRequest(t *testing.T) {
// 	type requestHandled struct {
// 		deviceID     string
// 		responseTime time.Duration
// 	}

// 	tests := []requestHandled{
// 		{
// 			deviceID:     "host_1",
// 			responseTime: 2000,
// 		},
// 		{
// 			deviceID:     "host_1",
// 			responseTime: 2000,
// 		},
// 		{
// 			deviceID:     "host_1",
// 			responseTime: 2000,
// 		},
// 		{
// 			deviceID:     "host_2",
// 			responseTime: 1500,
// 		},
// 		{
// 			deviceID:     "host_2",
// 			responseTime: 1500,
// 		},
// 		{
// 			deviceID:     "host_2",
// 			responseTime: 2000,
// 		},
// 		{
// 			deviceID:     "host_1",
// 			responseTime: 4000,
// 		},
// 		{
// 			deviceID:     "host_1",
// 			responseTime: 2000,
// 		},
// 		{
// 			deviceID:     "host_3",
// 			responseTime: 500,
// 		},
// 		{
// 			deviceID:     "host_4",
// 			responseTime: 4000,
// 		},
// 		{
// 			deviceID:     "host_5",
// 			responseTime: 1000,
// 		},
// 	}
// 	_ = tests

// 	ism := New()
// 	_ = ism
// 	ch := make(chan requestHandled)
// 	for i := 0; i < 5; i++ {
// 		go func(ch <-chan requestHandled) {
// 			for r := range ch {
// 				ism.NewRequest(r.deviceID, r.responseTime)
// 			}
// 		}(ch)
// 	}

// 	for _, tt := range tests {
// 		ch <- tt
// 	}
// 	close(ch)

// 	ism.Stop()
// 	time.Sleep(time.Second)

// 	expectedAvg := uint64(0)
// 	count := uint64(0)
// 	for _, tt := range tests {
// 		expectedAvg = uint64(math.Round(float64((expectedAvg*count)+tt.responseTime) / float64(count+1)))
// 		count++
// 	}
// 	assert.Equal(t, ism.avgResponseTime(), expectedAvg)
// }

func TestImpactPeek(t *testing.T) {
	pq := New()

	pq.Push(&Item{
		ID:       "host_3",
		Priority: 2,
	})

	pq.Push(&Item{
		ID:       "host_2",
		Priority: 3,
	})

	pq.Push(&Item{
		ID:       "host_1",
		Priority: 1,
	})

	pq.Push(&Item{
		ID:       "host_4",
		Priority: 0,
	})

	fmt.Printf("pop: %v\n", pq.Peek().(*Item).Priority)
	fmt.Printf("pop: %v\n", pq.Pop().(*Item).Priority)
	fmt.Printf("pop: %v\n", pq.Peek().(*Item).Priority)
	fmt.Printf("pop: %v\n", pq.Peek().(*Item).Priority)
	// fmt.Printf("peek: %v\n", pq.Peek().Priority)
}
