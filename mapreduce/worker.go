package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const (
	MAP = 0
	REDUCE = 1
)

type RegisterSend struct {
	Port int64
}

//type RegisterReply struct {
//	Err Err
//}

type TaskFinishedSend struct {
	TaskId int
	TaskType int
}

type MapStartSend struct {
	FMap func(string, string)[]KeyValue
	InputFile string
	TaskId int
}

type ReduceStartSend struct {
	FReduce func(string, []string)string
}

type Worker struct {
	mu sync.Mutex
	port int64
	masterPort int64

	fMap func(string, string) []KeyValue
	fReduce func(string, []string) string

	// Task id assigned to worker
	taskId int
}

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
	//fmt.Println(worker.fMap("txt", "contents are here"))
	//fmt.Println(content)
	go func() {
		for _, kv := range worker.fMap(args.InputFile, string(content)) {
			fmt.Println(kv.Key, kv.Value)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal("Unable To Encode Map Result")
			}
		}

		send := TaskFinishedSend{TaskId: args.TaskId, TaskType: MAP}
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

func (worker *Worker) StartReduce(args *ReduceStartSend, reply *GeneralReply) error {
	return nil
}

func (worker *Worker) StartWorker() {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	workerPC := rpc.NewServer()
	workerPC.Register(worker)

	l, e := net.Listen("tcp", ":"+strconv.FormatInt(worker.port, 10))
	if e != nil {
		log.Fatal("Master listen error:", e)
	}

	// Run server concurrently
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go workerPC.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()



	args := RegisterSend{worker.port}
	reply := GeneralReply{}
	Call(
		worker.masterPort,
		"Master.RegisterWorker",
		&args,
		&reply,
	)
}
