build:
	go build -o bin/client client/cmd/main.go
	go build -o bin/kv

proto:
	protoc -I=. --go_out=. raftpb/raft.proto

clean:
	rm -rf bin/
	rm -rf node-*