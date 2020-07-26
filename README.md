# Distributor - Distributed System Implementation Based On MapReduce Paper

This is a third party gadget tool to perform map-reduce on a single computer. Uses go concurrency to simulate individual node in a network cluster

## Map & Reduce Function

Users define map and reduce functions specified in the paper, and plug these two functions to Worker nodes in the clustor to perform large scale computing

map and reduce functions are defined as follows

```go
import (
    "math/rand"
    "strings"
    "time"
    "../mapreduce"
)

func mapFunc(key, value string) []mapreduce.KeyValue
func reduceFunc(key string, values []string) string
```

## Sample Usage

```go
package main

import (
    "math/rand"
    "strings"
    "time"

    "../mapreduce"
)

// Perform word count
// Only map part
func mapFunc(key, value string) []mapreduce.KeyValue {
    var kv []mapreduce.KeyValue
    for _, w := range strings.Split(value, " ") {
        kv = append(kv, mapreduce.KeyValue{Key: w, Value: "1"})
    }
    return kv
}

// Just a place holder
func mockReduce(key string, values []string) string {
    return "hello world"
}

func main() {
    files := []string{
        "dataset/d1.txt",
        "dataset/d2.txt",
        "dataset/d3.txt",
        "dataset/d4.txt",
        "dataset/d5.txt",
    }

    // Create master node given input, number of reduce tasks, and port it works on
    master := mapreduce.MakeMaster(files, 3, 4000)
    master.RunMaster()

    w1 := mapreduce.MakeWorker(3000, 4000, mapFunc, mockReduce)
    w1.StartWorker()
    w2 := mapreduce.MakeWorker(3001, 4000, mapFunc, mockReduce)
    w2.StartWorker()
    w3 := mapreduce.MakeWorker(3002, 4000, mapFunc, mockReduce)
    w3.StartWorker()

    mapreduce.WaitUntil(master.Done)
}
```

## Theory

Implemented most basic features of map-reduce.

There is a single master node that controls the whole map-reduce process. And there are numerous worker node to perform map or reduce operation.

Reduce operation must wait for all map operation to finish.

Master node will periodically check all registered worker node. If a registered worker does not respond, master node will remove this worker node, and assign the task of this worker to another worker

In the paper, the input must be pre-splitted. However, the input are already splited into different files, so master does not have to split it again

Once a task (map or reduce) assigned to a worker is finished, the worker will notify master node. If this task haven't been finished by other workers, this worker will atomically rename the temp file to map result (files used by reduce phase)

Each map result will be splited into n files, where n is the number of reduce tasks. For example, there m map inputs and n reduce tasks, then there will be m * n intermediate files produced by map and consumed by reduce

For example, 3 input files and 2 reduce tasks, then the intermediate files will be

```shell
mr-0-0
mr-0-1
mr-1-0
mr-1-1
mr-2-0
mr-2-1
```

And reduce node 0 will comsume mr-0-0, mr-1-0 and mr-2-0, and map node 0 will produce mr-0-0, mr-0-1
