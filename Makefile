protoc-go: 
	protoc -I. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative \
		--go_out=. --go-grpc_out=. ./proto/avdb.proto

build:
	go build -o ./cmd/server/avdb-server ./cmd/server/main.go  

start-demo:
	./cmd/server/avdb-server --host 127.0.0.1 --port 50051 --vector-dim 4 --run-demo 1 --size-demo 50000 

bench:
	go test -cpu 8 -bench '^Benchmark.*' -benchtime=20x -count=1
