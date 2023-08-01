package avdb

import (
	"fmt"
	pb "github.com/dmw2151/avdb/proto"
	"sync"
)

// localStore - a _very_ basic in-memory vector store
type localStore struct {
	refs map[string]*record
	head *record
	mut  sync.RWMutex
}

// record - internal record for vector w. metadata
type record struct {
	uuid string
	data []float64
	meta map[string]string
}

// scoredRecord - wrapper around record for handling results
type scoredRecord struct {
	record *record
	score  float64
}

// insert - insert a set of records into local store
func (ls *localStore) insert(rec *record) error {
	ls.mut.Lock()
	defer ls.mut.Unlock()

	// update in reference if already in DB - no link actions
	if orec, ok := ls.refs[rec.uuid]; ok {
		orec.meta = rec.meta
		orec.data = rec.data
		return nil
	}

	// refs management - any new entry
	ls.refs[rec.uuid] = rec

	// NOTE:
	// link management - first insert
	if ls.head == nil {
		ls.head = rec
		return nil
	}
	return nil
}

// fetch - single uuid lookup - get one or zero records from local store
func (ls *localStore) fetch(uuid string) *record {
	ls.mut.RLock()
	defer ls.mut.RUnlock()
	if rec, ok := ls.refs[uuid]; ok {
		return rec
	}
	return nil
}

// del - removes a record from the local store
func (ls *localStore) del(uuid string) (bool, error) {
	ls.mut.Lock()
	defer ls.mut.Unlock()
	if _, ok := ls.refs[uuid]; !ok {
		return false, nil
	} else {
		ls.refs[uuid] = nil
		delete(ls.refs, uuid)
	}

	return true, nil
}

// query - does a scan over the entire DB and returns top results by distance
func (ls *localStore) query(metric pb.Metric, search []float64, limit int) ([]*scoredRecord, error) {

	ls.mut.RLock()
	defer ls.mut.RUnlock()

	// get distancer from metric name
	distancer, ok := distanceMetricToDistancer[metric]
	if !ok {
		return []*scoredRecord{}, fmt.Errorf("unknown metric: %s", metric)
	}

	// set limit to min(dbsize, limit) && initialize PQ for the query
	if dbsize := len(ls.refs); dbsize < limit {
		limit = dbsize
	}

	var nearest = newprioQueue(limit)
	for _, cur := range ls.refs {
		nearest.push(&obj{value: cur, priority: distancer.distance(cur.data, search)})
	}

	// scan the PQ, grab the results w. priority
	var nearestScored = make([]*scoredRecord, limit)
	for i, rec := range nearest.objs {
		nearestScored[i] = &scoredRecord{
			record: rec.value, score: rec.priority,
		}
	}

	return nearestScored, nil
}
