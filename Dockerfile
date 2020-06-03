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
# Debug utilities
RUN apk update && apk add curl bash
COPY --from=builder /go/src/github.com/raft-kv-store/bin/client /bin/client
COPY --from=builder /go/src/github.com/raft-kv-store/bin/kv /bin/kv
COPY  config/shard-config.json config/shard-config.json
COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh && mkdir /logs
EXPOSE 17000 17001 17002 18000 18001 18002

# TODO Fix kv to have a single process that can be called to set everything up. It is an anti pattern to run multiple processes inside a container
CMD /bootstrap.sh
