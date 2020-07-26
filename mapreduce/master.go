// Copyright 2020 NeoClear. All rights reserved.
// Algorithms and data structures defined to make master work

package mapreduce

import (
    "log"
    "sync"
    "time"
)

type WorkerStatus int

type TaskId int
type TaskType int

// The status of worker machine
const (
    AVAILABLE = 0
    RUNNING   = 1
    FAILED    = 2
)

// The status of tasks
const (
    UNPROCESSED = 0
    PROCESSING  = 1
    FINISHED    = 2
)

// The return type of rpc
const (
    WASTE = "WASTE"
)

// The data structure that stores worker status
type WorkerRegistry struct {
    status WorkerStatus
    taskId TaskId
}

// The master data structure
type Master struct {
    // The lock
    mu sync.Mutex

    // The mapping that stores the status of registered workers
    workers map[int64]WorkerRegistry

    // The number of map tasks
    nMap int
    // The number of reduce tasks
    nReduce int
    // A list of input files
    inputFiles []string

    // Deprecated
    // User-defined map function
    // The map function takes a input file name and its content
    // And return a list of key-value pairs
    //fMap func(string, string) []KeyValue

    // Deprecated
    // User-defined reduce function
    // The reduce function takes a key and a list of value
    // And return the merged data of string type
    //fReduce func(string, []string) string

    // Mark the map task that is finished
    mapStatus        []int
    mapFinishedCount int
    // Mark the reduce task that is finished
    reduceStatus        []int
    reduceFinishedCount int

    // The port of master node
    port int64
}

// Create a new master node
// Init values
func MakeMaster(inputFiles []string, nReduce int, port int64) *Master {
    // Create and init master
    master := Master{}
    master.workers = map[int64]WorkerRegistry{}
    master.nMap = len(inputFiles)
    master.nReduce = nReduce
    master.inputFiles = inputFiles

    // Init task status
    master.mapStatus = make([]int, master.nMap)
    master.reduceStatus = make([]int, master.nReduce)

    master.port = port

    return &master
}

// Register workers to master
func (master *Master) RegisterWorker(args *RegisterSend,
    reply *GeneralReply) error {
    // Lock the register operation
    master.mu.Lock()
    defer master.mu.Unlock()

    // Register the worker with id
    // Initially available
    master.workers[args.Port] = WorkerRegistry{status: AVAILABLE}
    reply.Err = OK

    return nil
}

// rpc that indicates the task is finished (map or reduce)
func (master *Master) TaskFinished(args *TaskFinishedSend,
    reply *GeneralReply) error {

    master.mu.Lock()
    defer master.mu.Unlock()

    // Reference (or pointer) to store actual status array
    // And counter integer
    var statusRef *[]int
    var counter *int

    // Assign actual value to statusRef and counter
    switch args.TaskType {
    case MAP:
        // If the finished task type is map
        statusRef = &master.mapStatus
        counter = &master.mapFinishedCount
    case REDUCE:
        // If the finished task type is reduce
        statusRef = &master.reduceStatus
        counter = &master.reduceFinishedCount
    default:
        // If not match any task type, throw error
        log.Fatal("Unexpected Task Type")
    }

    // Mark worker as available
    master.workers[args.WorkerId] = WorkerRegistry{
       status: AVAILABLE,
       taskId: 0,
    }


    // If task already finished, reply WASTE
    if (*statusRef)[args.TaskId] == FINISHED {
        reply.Err = WASTE
        return nil
    }

    // Mark task as finished, and inc counter
    (*statusRef)[args.TaskId] = FINISHED
    *counter++

    reply.Err = OK
    return nil
}

// Execute the master
func (master *Master) RunMaster() {
    // Create the corresponding server
    rp, listener := CreateServer(master, master.port, "Master")

    // Run server concurrently
    go RunServer("Master", rp, listener)


    // Schedule tasks
    // Run map tasks
    // Then run reduce tasks
    go schedule(master)
}

// Return the port of available worker
// Return -1 if no worker is available
func (master *Master) getAvailableWorker() int64 {
    master.mu.Lock()
    defer master.mu.Unlock()

    for port, v := range master.workers {
        if v.status == AVAILABLE {
            return port
        }
    }
    return -1
}

func (master *Master) getStatusRef(taskType TaskType) *[]int {
    master.mu.Lock()
    defer master.mu.Unlock()

    var statusRef *[]int

    switch taskType {
    case MAP:
        statusRef = &master.mapStatus
    case REDUCE:
        statusRef = &master.reduceStatus
    default:
        log.Fatal("Unexpected Task Type")
    }

    return statusRef
}

// Return the unprocessed task id of task type
// Return -1 if no unprocessed task is found
func (master *Master) getUnprocessedTaskId(taskType TaskType) TaskId {
    // The reference to actual task status array
    statusRef := master.getStatusRef(taskType)

    master.mu.Lock()
    defer master.mu.Unlock()

    for idx, status := range *statusRef {
        if status == UNPROCESSED {
            return TaskId(idx)
        }
    }

    return -1
}

func (master *Master) setTaskStatus(id TaskId, taskType TaskType, status int) {
    statusRef := master.getStatusRef(taskType)

    master.mu.Lock()
    defer master.mu.Unlock()

    (*statusRef)[id] = status
}

func (master *Master) getTaskStatus(id TaskId, taskType TaskType) int {
    statusRef := master.getStatusRef(taskType)

    master.mu.Lock()
    defer master.mu.Unlock()

    return (*statusRef)[id]
}

func (master *Master) setWorkerStatus(workerId int64, status WorkerRegistry) {
    master.mu.Lock()
    defer master.mu.Unlock()

    master.workers[workerId] = status
}

func (master *Master) checkAvailableWorkerForTask(taskType TaskType) {
    for {
        // If task has already finished, then just quit
        // Because it is no longer necessary
        if master.PhaseFinished(taskType) {
            break
        }
        taskId := master.getUnprocessedTaskId(taskType)
        if taskId == -1 {
            Pause()
            continue
        }

        workerId := master.getAvailableWorker()
        if workerId == -1 {
            Pause()
            continue
        }

        master.setTaskStatus(taskId, taskType, PROCESSING)
        master.setWorkerStatus(workerId, WorkerRegistry{
            status: RUNNING,
            taskId: taskId,
        })

        master.mu.Lock()

        args := MapStartSend{
            InputFile: master.inputFiles[taskId],
            TaskId: taskId,
        }
        reply := GeneralReply{}

        master.mu.Unlock()

        Call(workerId, "Worker.StartMap", &args, &reply)

        Pause()
    }
}

func (master *Master) removeUnavailableWorker(taskType TaskType) {
    master.mu.Lock()
    defer master.mu.Unlock()

    for workId, _ := range master.workers {
        if !Call(workId, "Worker.IsOnline", &struct{}{}, &struct{}{}) {
            master.workers[workId] = WorkerRegistry{taskId: 0, status: FAILED}
            //if (master.workers[workId].taskId)
        }

        time.Sleep(time.Second)
    }
}

func (master *Master) MapFinished() bool {
    master.mu.Lock()
    defer master.mu.Unlock()
    return master.mapFinishedCount == master.nMap
}

func (master *Master) ReduceFinished() bool {
    master.mu.Lock()
    defer master.mu.Unlock()
    return master.reduceFinishedCount == master.nReduce
}

func (master *Master) PhaseFinished(taskType TaskType) bool {
    switch taskType {
    case MAP:
        return master.MapFinished()
    case REDUCE:
        return master.ReduceFinished()
    default:
        log.Fatal("Unexpected Task Type")
    }
    return false
}

// Check if the whole task has finished
func (master *Master) Done() bool {
    return master.MapFinished() && master.ReduceFinished()
}
