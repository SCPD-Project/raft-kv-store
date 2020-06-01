BUILD_VERSION ?= v0.1

.PHONY: build-local
# build-local is for backward compatibility which ensures that the built artifacts are present locally as well
build-local:
	go mod tidy
	CGO_ENABLED=0 GOARCH=amd64 go build -ldflags "-X main.GitCommit=$(git rev-parse --short HEAD)" -o bin/kv
	CGO_ENABLED=0 GOARCH=amd64 go build -ldflags "-X main.GitCommit=$(git rev-parse --short HEAD)" -o bin/client client/cmd/main.go

build: build-local
	docker build -t supriyapremkumar/kv:${BUILD_VERSION} .
proto:
	protoc -I=. --go_out=. raftpb/raft.proto

clean:
	rm -rf node-*
