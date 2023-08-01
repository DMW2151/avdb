package avdb

import (
	pb "github.com/dmw2151/avdb/proto"
	"math"
)

// distancer - interface for things that calculate distance
type distancer interface {
	distance(a, b []float64) float64
}

var distanceMetricToDistancer = map[pb.Metric]distancer{
	pb.Metric_L2: L2Distancer{},
}

// L2Distancer - L2 distance
type L2Distancer struct{}

// distance - implements distancer for L2 distancer, prefer accumulate dist * dist
// over math.Pow(dist, 2), roughly ~5x speedup to _avoid_ math.Pow(m, 2) !?
func (d L2Distancer) distance(a, b []float64) float64 {
	var dist float64
	var diff float64
	for i, _ := range a {
		diff = (a[i] - b[i])
		dist += diff * diff
	}
	return math.Sqrt(dist)
}
