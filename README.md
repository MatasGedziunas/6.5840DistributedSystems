# MIT Distributed systems 6.5840 lab works

## MapReduce LAB1
Finished.

### Architecture

The system consists of two main components:

1. **Coordinator**: A single process that manages the distribution of tasks to workers and handles fault tolerance.
2. **Workers**: Multiple processes that execute Map and Reduce tasks as assigned by the coordinator.

The coordinator and workers communicate via RPC (Remote Procedure Call). The system is designed to handle worker failures and can reassign tasks if a worker doesn't complete its assigned task within a specified timeout period.

### Testing

To run the test script for different scenarios (Word count test, Indexer test, Map parallelism test, Reduce parallelism test, Job count test, Early exit test, Crash recovery test):

```
cd ~/6.5840/src/main
bash test-mr.sh
```

To test independently with whatever task (eg. wc):

- Build the task

```
cd ~/6.5840/src/main
go build -buildmode=plugin ../mrapps/wc.go
```

- Run the master and workers

```
go run mrcoordinator.go pg-*.txt
```

In different terminals:
```
go run mrworker.go wc.so
```

## Key-Value server with request duplicate detection Lab2

The code does not pass the tests, but I have no clue what is wrong, tell me :P

In folder: src/kvsrv

## Raft Lab3

In file util.go it is possible to turn debugging on or off.

### Leader-election

Implemented leader-election and heartbeat sending functionality, you can test this code by running:

```
cd src/raft
go test -race -run 3A
```
