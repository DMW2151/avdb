package avdb

import (
	"context"
	"fmt"
	pb "github.com/dmw2151/avdb/proto"
	"math"
	"math/rand"
	"sync"

	uuid "github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// default to L2 distancer for nearest calcs
var defaultL2Distancer = distanceMetricToDistancer[pb.Metric_L2]

// Server - implements a local, in-memory vector db store
type Server struct {
	stores   map[string]*localStore
	nStores  int
	storeIds []string
	pb.UnimplementedAVDBServer
}

// NewServer -
func NewServer(n int) *Server {

	var (
		stores   = make(map[string]*localStore, n)
		storeIds = make([]string, n)
		id       string
	)

	for i := 0; i < n; i++ {
		id = uuid.Must(uuid.NewRandom()).String()
		stores[id] = &localStore{
			refs: make(map[string]*record),
			mut:  sync.RWMutex{},
		}
		storeIds[i] = id
	}
	return &Server{
		stores:   stores,
		nStores:  n,
		storeIds: storeIds,
	}
}

// Insert - inserts a _single_ record into storage
//
// TODO: inserts should NOT be random!! fine now since we're calling reindex after insert, should
// always go to nearest centroid...
func (s *Server) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	var r = &record{uuid: req.Record.Uuid, meta: req.Record.Meta, data: req.Record.Data}
	var nearest = s.storeIds[rand.Intn(s.nStores)]
	if err := s.stores[nearest].insert(r); err != nil {
		return nil, status.Error(codes.Internal, err.Error()) // TODO: error codes
	}
	return &pb.InsertResponse{Success: true}, nil
}

// Reindex - runs approximate k-means++ on all nodes using the procedure described below.
//
// Arthur and Vassilvitskii modified the initialization step in a careful manner and obtained a
// randomized initialization algorithm called k-means++. The main idea in their algorithm is to
// choose the centers one by one in a controlled fashion, where the current set of chosen centers
// will stochastically bias the choice of the next center. The advantage of k-means++ is that even
// the initialization step itself obtains an (8 log k)-approximation.
//
// src: https://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf
func (s *Server) Reindex(ctx context.Context, req *pb.ReindexRequest) (*pb.ReindexResponse, error) {

	// TODO: add support for changing size on reindex, right now you get a _fixed_ number of
	// stores.

	// init full record count of db, need _exact_ count for sampling weights
	var dbsize int
	for _, id := range s.storeIds {
		dbsize += len(s.stores[id].refs)
	}

	var centroids = &localStore{
		refs: make(map[string]*record),
		mut:  sync.RWMutex{},
	}

	// seed centroids (C) w. a random first point...
	var randStoreUUID = s.storeIds[rand.Intn(s.nStores)]
	var initCentroid = s.stores[randStoreUUID].head

	if err := centroids.insert(initCentroid); err != nil {
		return &pb.ReindexResponse{Success: false}, status.Error(codes.Internal, err.Error())
	}

	// sample for remaining centroids by calculating min distance to any c in C
	// for all points, sample proportional to the distance. Do this K times.
	for i := 1; i < s.nStores; i++ {
		var (
			weightsref = make(map[int]*record, dbsize)
			weights    = make([]float64, dbsize)
			ctr        int
			sq         float64
			sumsq      float64
		)

		// This could be improved w. a cache? Do not _need_ to recalculate every point each
		// time, or shouldn't....
		// NOTE: K == s.nStores, see comment @ top of function
		for _, id := range s.storeIds {
			for _, rec := range s.stores[id].refs {
				if best, _ := centroids.query(pb.Metric_L2, rec.data, 1); len(best) == 1 {
					sq = best[0].score * best[0].score
					sumsq += sq
					weights[ctr] = sq
					weightsref[ctr] = rec
				} else {
					weightsref[ctr] = nil
				}
				ctr++
			}
		}

		// this is a low-effort randomized choice w. weights, select uniform on (0, sum dist^2)
		// and select the point that covers the rand point.
		var (
			rpoint = rand.Float64() * sumsq
			cumsum float64
			choice int
		)

		for i, w := range weights {
			cumsum += w
			if cumsum > rpoint {
				choice = i
				break
			}
		}

		// insert into C -> next iter.
		if err := centroids.insert(weightsref[choice]); err != nil {
			return &pb.ReindexResponse{Success: false}, status.Error(codes.Internal, err.Error())
		}
	}

	// TODO: fix...
	// one final pass to re-assign points to nearest centroid, waste a lot of mem here, oh well
	// get uuid of nearest centroids, assign new stores onto stores at end...
	var newstores = make(map[string]*localStore, s.nStores)

	for k, v := range centroids.refs {
		newstores[k] = &localStore{
			refs: make(map[string]*record),
			mut:  sync.RWMutex{},
			head: v,
		}
	}

	for _, id := range s.storeIds {
		for _, rec := range s.stores[id].refs {
			nearest, err := centroids.query(pb.Metric_L2, rec.data, 1)
			if err != nil {
				return &pb.ReindexResponse{Success: false}, status.Error(codes.Internal, err.Error())
			}

			var newStoreId = nearest[0].record.uuid // TODO: check this exists...
			newstores[newStoreId].insert(rec)
		}
	}

	// replace dataset of ptr to stores
	s.stores = newstores

	// replace listing of centroids
	var ctr int
	for id, _ := range centroids.refs {
		s.storeIds[ctr] = id
		ctr++
	}

	return &pb.ReindexResponse{Success: true}, nil
}

// Fetch - gets a record from storage w. uuid
func (s *Server) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	for _, id := range s.storeIds {
		if resp := s.stores[id].fetch(req.Uuid); resp != nil {
			return &pb.FetchResponse{
				Record: &pb.Record{Uuid: resp.uuid, Meta: resp.meta},
			}, nil
		}
	}
	return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to fetch record (%s) from local store", req.Uuid))
}

// Delete - deletes a record from storage w. uuid
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	for _, id := range s.storeIds {
		if ok, _ := s.stores[id].del(req.Uuid); ok {
			return &pb.DeleteResponse{Uuid: req.Uuid, Success: ok}, nil
		}
	}
	return &pb.DeleteResponse{Uuid: req.Uuid, Success: false}, nil
}

// Query - queries storage using an reference vector, returns the N closest results
func (s *Server) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {

	// TODO: add a warn or error here
	// when probes is empty or 0, set to 1. when too high, set to search all regions
	if req.Probes == 0 {
		req.Probes = 1
	}

	if req.Probes > int64(s.nStores) {
		req.Probes = int64(s.nStores)
	}

	// init PQ && determine nearest regions to search
	var (
		clustersToSearch = newprioQueue(int(req.Probes))
		distr            = L2Distancer{}
		distance         float64
	)

	for _, id := range s.storeIds {
		distance = distr.distance(s.stores[id].head.data, req.Data)
		clustersToSearch.push(&obj{value: &record{uuid: id}, priority: distance})
	}

	// init global PQ of top results, PQ gets req.Limit results for each probe / region
	// and keeps the best req.Limit overall
	var globaltop = newprioQueue(int(req.Limit))
	var wg = sync.WaitGroup{}

	// query each of req.Probes nearest regions and send results to PQ
	for _, rec := range clustersToSearch.objs {
		wg.Add(1)
		go func(id string, wg *sync.WaitGroup) {
			defer wg.Done()

			// TODO: handle errors, single regions may fail...
			regiontop, _ := s.stores[id].query(req.Metric, req.Data, int(req.Limit))
			globaltop.m.Lock()
			defer globaltop.m.Unlock()
			for _, res := range regiontop {
				globaltop.push(&obj{value: res.record, priority: res.score})
			}
		}(rec.value.uuid, &wg)
	}
	wg.Wait()

	// compose results from PQ into pb.QueryResponse, skipping over inf. values
	var globalresults = make([]*pb.Record, int(req.Limit))
	for i, rec := range globaltop.objs {
		if rec.priority == math.Inf(1) {
			continue
		}

		globalresults[i] = &pb.Record{
			Uuid:  rec.value.uuid,
			Data:  rec.value.data,
			Meta:  rec.value.meta,
			Score: rec.priority,
		}
	}
	return &pb.QueryResponse{Records: globalresults}, nil
}
