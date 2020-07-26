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

//type RegisterReply struct {
//	Err Err
//}

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
    mu         sync.Mutex
    port       int64
    masterPort int64

    fMap    func(string, string) []KeyValue
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

func (worker *Worker) StartReduce(args *ReduceStartSend, reply *GeneralReply) error {
    return nil
}

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

func (worker *Worker) IsOnline(_, _ *struct{}) error {
    return nil
}
