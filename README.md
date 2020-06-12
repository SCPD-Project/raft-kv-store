# RAFT based Key-Value Store with Transaction Support
[![SCPD-Project](https://circleci.com/gh/SCPD-Project/raft-kv-store/tree/master.svg?style=shield)](https://circleci.com/gh/SCPD-Project/raft-kv-store/tree/master)

## Description
In this project, we present a highly available, consistent, fault tolerant distributed Key-Value Store. 
It adapts the RAFT consensus algorithm broadly in the system and supports concurrent transactions across shards. 
Each operation on the store is handled by a set of coordinators as a RAFT group. 

The keys in the store are partitioned across shards and each shard maintains multiple replicas forming their own RAFT groups. 
The distributed transactions across shards is achieved using two-phase commit protocol with two-phase locking to guarantee atomicity and serializability.

## Dependencies
[docker](https://docs.docker.com/get-docker/) runtime is the only dependency to build and run



## Build kv container
```
make build
```

## Bootstrap cluster
This drops into a client container shell to perform kv CRUDs
```
make cluster
 ######################### Starting Client #########################
>
>
>txn
Entering transaction status
>set universe 42
>set team 4
>end
Submitting [method:"set" key:"universe" value:42 method:"set" key:"team" value:4]
OK
>get team
Key=team, Value=4
>get universe
Key=universe, Value=42
```

## Client commands:
- `get [key]`: get value of a key from RAFT KV store
  - Examples: `get class` or `get "distributed system"`
  - If the `[key]` does not exist, return message `Key=[key] does not exist`
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
- `add [key] [value]`: add value to an existing key 
  - Example: `add "my account" 100`
- `sub [key] [value]`: subtract value to an existing key 
  - Example: `sub "my account" 50`
- `xfer [from-key] [to-key] [value]`: transfer value from one key to another
  - Example: `xfer bank-A bank-B 50` 
  - If either of `[key]` does not exist, return message `Key=[key] does not exist`  
  - If `[from-key]` has a current value less than `[value]`, return message `Insufficient funds`
- `exit`: exit client from server

## Performance test
To run the performance test locally:
```bazaar
go run metric/performance.go
```
To run the performance in the client container:
```bazaar
make test-performance
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
