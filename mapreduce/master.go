package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type WorkerStatus int

const (
	AVAILABLE = 0
	RUNNING   = 1
	FAILED    = 2
)

const (
	UNPROCESSED = 0
	PROCESSING = 1
	FINISHED = 2
)

const (
	WASTE = "WASTE"
)

type WorkerRegistry struct {
	status WorkerStatus
	taskId int
}

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

	// User-defined map function
	// The map function takes a input file name and its content
	// And return a list of key-value pairs
	fMap func(string, string) []KeyValue

	// User-defined reduce function
	// The reduce function takes a key and a list of value
	// And return the merged data of string type
	fReduce func(string, []string) string

	// Mark the map task that is finished
	mapStatus []int
	mapFinishedCount int
	// Mark the reduce task that is finished
	reduceStatus []int
	reduceFinishedCount int
}

func MakeMaster(inputFiles []string, nReduce int,
	fMap func(string, string) []KeyValue,
	fReduce func(string, []string) string) *Master {
	// Create and init master
	master := Master{}
	master.workers = map[int64]WorkerRegistry{}
	master.nMap = len(inputFiles)
	master.nReduce = nReduce
	master.inputFiles = inputFiles
	master.fMap = fMap
	master.fReduce = fReduce

	master.mapStatus = make([]int, master.nMap)
	master.reduceStatus = make([]int, master.nReduce)

	return &master
}

func (master *Master) RegisterWorker(args *RegisterSend,
	reply *GeneralReply) error {
	// Lock the register operation
	master.mu.Lock()
	defer master.mu.Unlock()

	// Register the worker with id
	// Initially available
	master.workers[args.Port] = WorkerRegistry{status: AVAILABLE}

	return nil
}

func (master *Master) TaskFinished(args *TaskFinishedSend,
	reply *GeneralReply) error {

	master.mu.Lock()
	defer master.mu.Unlock()
	switch args.TaskType {
	case MAP:
		if master.mapStatus[args.TaskId] == FINISHED {
			reply.Err = WASTE
			return nil
		}
		master.mapStatus[args.TaskId] = FINISHED
		master.mapFinishedCount++
	case REDUCE:
		if master.reduceStatus[args.TaskId] == FINISHED {
			reply.Err = WASTE
			return nil
		}
		master.reduceStatus[args.TaskId] = FINISHED
		master.reduceFinishedCount++
	default:
		log.Fatal("Unexpected Task Type")
	}

	reply.Err = OK
	return nil
}

func (master *Master) RunServer(port int64) {
	remotePC := rpc.NewServer()
	remotePC.Register(master)

	l, e := net.Listen("tcp", ":"+strconv.FormatInt(port, 10))
	if e != nil {
		log.Fatal("Master listen error:", e)
	}

	// Run server concurrently
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go remotePC.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
	go schedule(master)
}

func (master *Master) checkAvailableWorkerForTask(taskType int) {
	for {
		master.mu.Lock()

		switch taskType {
		case MAP:
			for idx, status := range master.mapStatus {
				if status == UNPROCESSED {
					for port, v := range master.workers {
						if v.status == AVAILABLE {
							args := MapStartSend{
								FMap: master.fMap,
								InputFile: master.inputFiles[idx],
								TaskId: idx,
							}
							reply := GeneralReply{}
							Call(port, "Worker.StartMap", &args, &reply)
							break
						}
					}
				}
			}
		case REDUCE:
		default:
			log.Fatal("Unexpected Task Type")
		}

		master.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (master *Master) removeUnavailableWorker(taskType int) {

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

func (master *Master) Done() bool {
	master.mu.Lock()
	defer master.mu.Unlock()

	return master.nMap == master.mapFinishedCount &&
		master.nReduce == master.reduceFinishedCount
}
