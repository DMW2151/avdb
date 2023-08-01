package main

import (
	"context"
	"flag"
	"fmt"
	//"math"
	"math/rand"
	"net"
	"sync"
	"time"

	avdb "github.com/dmw2151/avdb"
	pb "github.com/dmw2151/avdb/proto"
	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	addr       = flag.String("host", "127.0.0.1", "server's listening address")
	port       = flag.Int("port", 50051, "port to listen on for grpc requests")
	dimension  = flag.Int("vector-dim", 4, "size of vectors")
	demo       = flag.Int("run-demo", 1, "[demo] run demo workers on startup")
	size       = flag.Int("size-demo", 2000, "[demo] number of insert requests on startup")
	sourceFile = flag.String("file", "", "[demo] read file from disk on startup")
)

// newRandNormRecord - generates a _single_ random record
func newRandNormRecord(src rand.Source, dimension int) *pb.Record {
	var data = make([]float64, dimension)
	var uuid = uuid.Must(uuid.NewRandom()).String()
	for j := 0; j < dimension; j++ {
		data[j] = rand.NormFloat64()
	}
	return &pb.Record{Uuid: uuid, Data: data}
}

func main() {
	flag.Parse()

	var suggestedNodes = int(float64(*size) * 1 / 1000) // or could be int(math.Sqrt(float64(*size)))
	var srv = avdb.NewServer(suggestedNodes)            // use the pg-vector recommended value here..

	grpcServer := grpc.NewServer([]grpc.ServerOption{}...)
	pb.RegisterAVDBServer(grpcServer, srv)
	reflection.Register(grpcServer) // enable reflection for grpccurl

	var listenAddr = fmt.Sprintf("%s:%d", *addr, *port)
	lis, _ := net.Listen("tcp", listenAddr)
	log.WithFields(log.Fields{
		"addr": listenAddr,
	}).Info("started avdb server")

	// run the demo inserts in the background...
	if *demo > 0 {

		var (
			requests          = *size
			nworkers          = 16
			requestsPerWorker = requests / nworkers
			wg                = sync.WaitGroup{}
		)

		log.WithFields(log.Fields{
			"requests":            requests,
			"nworkers":            nworkers,
			"requests_per_worker": requestsPerWorker,
		}).Info("starting demo data write")

		for i := 0; i < nworkers; i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				var ctx = context.Background()
				var src = rand.NewSource(int64(time.Now().Nanosecond()))
				var rec *pb.Record
				for i := 0; i < requestsPerWorker; i++ {
					rec = newRandNormRecord(src, *dimension)

					if _, err := srv.Insert(ctx, &pb.InsertRequest{
						Record: &pb.Record{Uuid: rec.Uuid, Data: rec.Data},
					}); err != nil {
						log.Error(err.Error())
					}

				}
			}(&wg)
		}

		wg.Wait()
		log.Info("finished demo data write")

		// TODO: certain there's a data race w.o this line here
		time.Sleep(time.Millisecond * 100)
		log.Info("reindexing db")
		if r, err := srv.Reindex(context.Background(), &pb.ReindexRequest{Metric: pb.Metric_L2}); err != nil {
			log.Error(err.Error())
		} else {
			log.WithFields(log.Fields{"success": r.Success}).Info("reindex finished")
		}
	}

	if err := grpcServer.Serve(lis); err != nil {
		log.Error(err.Error())
	}
}
