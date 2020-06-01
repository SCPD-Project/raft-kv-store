# RAFT based Key-Value Store with Transaction Support

## Description
In this project, we present a highly available, consistent, fault tolerant distributed Key-Value Store. 
It adapts the RAFT consensus algorithm broadly in the system and supports concurrent transactions across shards. 
Each operation on the store is handled by a set of coordinators as a RAFT group. 

The keys in the store are partitioned across shards and each shard maintains multiple replicas forming their own RAFT groups. 
The distributed transactions across shards is achieved using two-phase commit protocol with two-phase locking to guarantee atomicity and serializability.

## Dependencies
[docker](https://docs.docker.com/get-docker/) runtime is the only dependency to build and run



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
bin/kv -i node-0 -l :11000 -r :12000
bin/kv -i node-1 -l :11001 -r :12001 -j :11000 
bin/kv -i node-2 -l :11002 -r :12002 -j :11000
```

## Start KV Shard-2
```
bin/kv -i node-3 -l :15000 -r :16000
bin/kv -i node-4 -l :15001 -r :16001 -j :15000 
bin/kv -i node-5 -l :15002 -r :16002 -j :15000
```

## Start Coordinator (Only 1 for now)
```
bin/kv -i node-6 -l :17000 -r :18000 -c
bin/kv -i node-7 -l :17001 -r :18001 -c -j :17000
bin/kv -i node-8 -l :17002 -r :18002 -c -j :17000
```

## Start Client
```
bin/client -e :17000
```
Client commands:
- `get [key]`: get value from RAFT KV store
  - Examples: `get class` or `get "distributed system"`
- `set [key] [value]`: put (key, value) on RAFT KV store
  - Examples: `put universe 42` or `put "2020 spring class students" 100`
- `del [key]`: delete key from RAFT KV store
  - Examples: `del class` or `del "distributed system"`
- `txn`: start a transaction (Only `set` and `del` are supported in transaction)
- `endtxn`: end a transaction
  - Example:
   ```bazaar
   txn 
   set universe 42
   set team 4
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

## License

    Copyright [2020] [Chen Chen, Varun Kulkarni, Supriya Premkumar, Renga Srinivasan]

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.