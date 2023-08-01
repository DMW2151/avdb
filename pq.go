package avdb

import (
	"math"
	"sort"
	"sync"
)

// obj - the object we hold in a prioQueue
type obj struct {
	value    *record
	priority float64
}

// prioQueue - not a proper PQ implementation, but maintains a small, sorted collection
// based on priority
type prioQueue struct {
	objs []*obj
	m    sync.RWMutex
}

// newprioQueue - create a new PQ, initialized w. inf values
func newprioQueue(lim int) *prioQueue {
	var pqobjs = make([]*obj, lim)
	for i, _ := range pqobjs {
		pqobjs[i] = &obj{value: nil, priority: math.Inf(1)}
	}
	return &prioQueue{
		objs: pqobjs,
		m:    sync.RWMutex{},
	}
}

// Len - implements sort.Interface
func (pq prioQueue) Len() int {
	return len(pq.objs)
}

// Swap - implements sort.Interface interface
func (pq prioQueue) Swap(i, j int) {
	pq.objs[i], pq.objs[j] = pq.objs[j], pq.objs[i]
}

// Less - implements sort.Data interface
func (pq prioQueue) Less(i, j int) bool {
	return pq.objs[i].priority < pq.objs[j].priority
}

// push - push onto PQ. Min. value for acceptance always decreasing! This seems like it does
// full sorts too much, and it _feels_ bad, but I think this is OK. Expect sort cost approx:
//
//	 	~ K^2 * log(K) * Harmonic_Num( # records scanned )
//		~ K*2 * log(K) * (log(N) + 1)
//
// For a top 10 query that's scanning 100,000 items, expect ~120 sorts of the PQ,
// polynomial in # items requested, logarithmic in N. meh, prefer this to the
// heap.Interface implementation from https://pkg.go.dev/container/heap.
func (pq prioQueue) push(o *obj) {
	if o.priority < pq.objs[len(pq.objs)-1].priority {
		pq.objs[len(pq.objs)-1] = o
		sort.Sort(pq)
	}
}
