FROM golang:1.14.3-alpine3.11
RUN apk update && apk add git \
                  curl \
                  protobuf-dev \
                  build-base
RUN go get -u github.com/mgechev/revive \
    golang.org/x/tools/cmd/goimports \
    github.com/golang/protobuf/protoc-gen-go
COPY revive.toml revive.toml
ENTRYPOINT sh
