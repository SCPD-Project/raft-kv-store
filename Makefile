.PHONY: build
build:
	go build -o bin/kv
	go build -o bin/client client/cmd/main.go

proto:
	protoc -I=. --go_out=. raftpb/raft.proto

clean:
	rm -rf node-*
