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

cluster: cluster-clean
	@docker network create raft-net  --subnet 10.10.10.0/24 || true
	mkdir -p node0 node1 node2 client
	docker run -d -e BOOTSTRAP_LEADER=yes -p 17000:17000 -v ${PWD}/node0:/pv/  --rm --net raft-net --hostname node0 --name node0 supriyapremkumar/kv:v0.1
	docker run -d -e BOOTSTRAP_FOLLOWER=yes -p 17001:17001 -v ${PWD}/node1:/pv/ --rm --net raft-net --hostname node1 --name node1 supriyapremkumar/kv:v0.1
	docker run -d -e BOOTSTRAP_FOLLOWER=yes -p 17002:17002 -v ${PWD}/node2:/pv/ --rm --net raft-net --hostname node2 --name node2 supriyapremkumar/kv:v0.1
	@printf "\n\n ######################### Starting Client ######################### \n\n"
	@docker run -it --net raft-net --hostname client --name client supriyapremkumar/kv:v0.1 client -e node0:17000

cluster-clean:
	docker rm -fv node0 node1 node2 client || true
	
clean:
	rm -rf node-*
	rm -rf cohort*
