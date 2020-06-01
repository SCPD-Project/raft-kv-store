FROM supriyapremkumar/builder:v0.1 as builder
MAINTAINER Supriya Premkumar <bWUgYXQgc3Vwcml5YSBkb3QgZGV2Cg==>

WORKDIR $GOPATH/src/github.com/raft-kv-store

ADD go.mod go.sum $GOPATH/src/github.com/raft-kv-store/
RUN go mod download

COPY . .
RUN go mod tidy
RUN make proto
RUN make build-local

FROM alpine:3.11
COPY --from=builder /go/src/github.com/raft-kv-store/bin/client /bin/client
COPY --from=builder /go/src/github.com/raft-kv-store/bin/kv /bin/kv
COPY  config/shard-config.json config/shard-config.json
CMD kv
