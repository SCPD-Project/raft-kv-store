# RAFT-KV-Store

## Build Protobuf
```
protoc -I=. --go_out=. raftpb/raft.proto
```

## Start
```
go build -o bin/kv
bin/kv -i node-0
bin/kv -i node-1 -l :11001 -r :12001 -j :11000
bin/kv -i node-2 -l :11002 -r :12002 -j :11000
```

## Leader:
```
curl localhost:11000/leader
```

## Put
```
curl -v localhost:11001/key -d '{"class-3": "cs244b5"}'
```

## Get:
```
curl -v localhost:11002/key/class-3
```

## Transactions:
```
curl -vvv localhost:11001/transaction -d '{"commands": [{"Command": "set", "Key": "name", "Value": "John"},{"Command": "set", "Key": "timezone", "Value": "pst"}]}'
```

##Get:
```
curl -vvv localhost:11002/key/timezone
```
