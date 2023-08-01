# A Vector DB

A lazy vector DB implementation, dumbest possible indexing, L2 distance only

## Demo Commands

start server w. 50K randomized vectors of size 4 to demonstrate this _mostly_ works

```bash
go run ./cmd/server/main.go --host 127.0.0.1 --port 50051 --vector-dim 4 --run-demo 1 --size-demo 50000 

~/go/bin/grpcurl -plaintext -d '{"limit": 1, "metric": "L2", "probes": 5, "data": [0.1, 0.1, 0.1, 1.5]}' \
	localhost:50051 avdb.AVDB/Query
```
