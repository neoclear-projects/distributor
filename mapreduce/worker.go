package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	MAP    = 0
	REDUCE = 1
)

type RegisterSend struct {
	Port int64
}

type TaskFinishedSend struct {
	TaskId   TaskId
	TaskType TaskType
	WorkerId int64
}

type MapStartSend struct {
	InputFile string
	TaskId    TaskId
	ReduceNum int
}

type ReduceStartSend struct {
}

type Worker struct {
	// The lock
	mu sync.Mutex

	// The address (port) of master and worker
	port       int64
	masterPort int64

	// User-defined map & reduce function
	fMap    func(string, string) []KeyValue
	fReduce func(string, []string) string

	// Task id assigned to worker
	taskId int
}

// Instantiate Worker object
func MakeWorker(port, masterPort int64,
	fMap func(string, string) []KeyValue,
	fReduce func(string, []string) string) *Worker {
	worker := Worker{}

	// Init ports
	worker.port = port
	worker.masterPort = masterPort

	worker.fMap = fMap
	worker.fReduce = fReduce

	return &worker
}

// Start map task
func (worker *Worker) StartMap(args *MapStartSend, reply *GeneralReply) error {

	content, err := ioutil.ReadFile(args.InputFile)
	if err != nil {
		log.Fatal("Cannot Open Map Source")
	}

	tempFile, e := ioutil.TempFile("", "distributor")
	if e != nil {
		log.Fatal("Cannot Create Temp File")
	}

	enc := json.NewEncoder(tempFile)

	go func() {
		for _, kv := range worker.fMap(args.InputFile, string(content)) {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal("Unable To Encode Map Result")
			}
		}

		send := TaskFinishedSend{TaskId: args.TaskId, TaskType: MAP, WorkerId: worker.port}
		result := GeneralReply{}

		Call(worker.masterPort, "Master.TaskFinished", &send, &result)

		if result.Err == OK {
			name := tempFile.Name()
			//fmt.Println(name)
			tempFile.Close()
			os.Rename(name, "mapresult/mr-"+strconv.FormatInt(int64(args.TaskId), 10))
		}
	}()

	return nil
}

// Start reduce function
func (worker *Worker) StartReduce(args *ReduceStartSend, reply *GeneralReply) error {
	return nil
}

// Start the worker
func (worker *Worker) StartWorker() {
	rp, listener := CreateServer(worker, worker.port, "Worker")

	// Run worker server concurrently
	go RunServer("Worker", rp, listener)

	Call(
		worker.masterPort,
		"Master.RegisterWorker",
		&RegisterSend{worker.port},
		&GeneralReply{},
	)
}

// A function used by master to check if client is still online
func (worker *Worker) IsOnline(_, _ *struct{}) error {
	return nil
}
