# RAFT-KV-Store

```
go build -o kv
./kv -id node0 ~/node-0
./kv -id node1 -haddr :11001 -raddr :12001 -join :11000 ~/node-1
./kv -id node2 -haddr :11002 -raddr :12002 -join :11000 ~/node-2

Leader:
curl localhost:11000/leader

Put
curl -v localhost:11001/key -d '{"class-3": "cs244b5"}'

Get:
curl -v localhost:11002/key/class-3

Transactions:
curl -vvv localhost:11001/transaction -d '{"commands": [{"Command": "set", "Key": "name", "Value": "John"},{"Command": "set", "Key": "timezone", "Value": "pst"}]}'

Get:
curl -vvv localhost:11002/key/timezone
```
