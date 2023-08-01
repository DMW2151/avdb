package avdb

import (
	"context"
	"math"
	"math/rand"
	"testing"

	pb "github.com/dmw2151/avdb/proto"
	uuid "github.com/google/uuid"
)

var benchmarkScenarios = []struct {
	name       string
	dimension  int
	DBSize     int
	partitions int
	metric     pb.Metric
	nresults   int
}{
	{
		name:       "768d_db_10k_top_1_with_partitions",
		dimension:  768,
		DBSize:     10000,
		partitions: int(math.Sqrt(10000)),
		metric:     pb.Metric_L2,
		nresults:   1,
	},
	{
		name:       "768d_db_10k_top_1_no_partition",
		dimension:  768,
		DBSize:     10000,
		partitions: 1,
		metric:     pb.Metric_L2,
		nresults:   1,
	},
	{
		name:       "768d_db_100k_top_1_with_partitions",
		dimension:  768,
		DBSize:     100000,
		partitions: int(math.Sqrt(100000)),
		metric:     pb.Metric_L2,
		nresults:   1,
	},
	{
		name:       "768d_db_100k_top_1_no_partition",
		dimension:  768,
		DBSize:     100000,
		metric:     pb.Metric_L2,
		partitions: 1,
		nresults:   1,
	},
}

// Benchmark_RPC_Query - benchmark query implementation
func Benchmark_RPC_Query(b *testing.B) {

	for _, bm := range benchmarkScenarios {
		b.Run(bm.name, func(b *testing.B) {

			b.StopTimer()
			srv := NewServer(bm.partitions)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for i := 0; i < bm.DBSize; i++ {
				var data = make([]float64, bm.dimension)
				var uuid = uuid.Must(uuid.NewRandom()).String()
				for j := 0; j < bm.dimension; j++ {
					data[j] = rand.NormFloat64()
				}
				srv.Insert(ctx, &pb.InsertRequest{Record: &pb.Record{Uuid: uuid, Data: data}})
			}
			b.StartTimer()

			var req *pb.QueryRequest
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				var data = make([]float64, bm.dimension)
				for j := 0; j < bm.dimension; j++ {
					data[j] = rand.NormFloat64()
				}
				req = &pb.QueryRequest{Data: data, Limit: int64(bm.nresults), Metric: bm.metric}
				b.StartTimer()

				srv.Query(ctx, req)
			}
		})
	}
}
