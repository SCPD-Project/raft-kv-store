# RAFT-KV-Store

## Build Protocol Buffer
```
make proto
```

## Build Program
```
make build
```

## Start KV Shard-1
```
bin/kv -i node-0 -l :11000 -r :12000 -p :13000
bin/kv -i node-1 -l :11001 -r :12001 -j :11000 -p :13001
bin/kv -i node-2 -l :11002 -r :12002 -j :11000 -p :13002
```

## Start KV Shard-2
```
bin/kv -i node-3 -l :15000 -r :16000 -p :14000
bin/kv -i node-4 -l :15001 -r :16001 -j :16000 -p :14001
bin/kv -i node-5 -l :15002 -r :16002 -j :16000 -p :14002
```

## Start Coordinator (Only 1 for now)
```
bin/kv -i node-6 -l :17000 -r :18000 -c
```

## Start Client
```
bin/client -e :17000
```
Client commands:
- `get [key]`: get value from RAFT KV store
  - Examples: `get class` or `get "distributed system"`
- `put [key] [value]`: put (key, value) on RAFT KV store
  - Examples: `put class cs244b` or `put "2020 spring class" "distributed system"`
- `del [key]`: delete key from RAFT KV store
  - Examples: `del class` or `del "distributed system"`
- `txn`: start a transaction (Only `set` and `del` are supported in transaction)
- `endtxn`: end a transaction
  - Example:
   ```bazaar
   txn 
   put class cs244b
   put univ stanford
   del class
   end
   ```
- `exit`: exit client from server

## Leader:
```
curl localhost:17000/leader
```

## Put
```
curl -v localhost:17000/key -d '{"class-3": "cs244b5"}'
```

## Get:
```
curl -v localhost:17000/key/class-3
```

## Transactions:
```
curl -vvv localhost:17000/transaction -d '{"commands": [{"Command": "set", "Key": "name", "Value": "John"},{"Command": "set", "Key": "timezone", "Value": "pst"}]}'
```

## Get:
```
curl -vvv localhost:17000/key/timezone
```
