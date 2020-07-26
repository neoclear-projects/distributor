package mapreduce

import (
    "encoding/json"
    "io/ioutil"
    "log"
    "os"
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

func createTemps(num int) []*os.File {
    var tempFile *os.File
    var result []*os.File
    var err error

    for i := 0; i < num; i++ {
        tempFile, err = ioutil.TempFile("", "distributor")
        if err != nil {
            log.Fatal("Cannot Create Temp File")
        }
        result = append(result, tempFile)
    }
    return result
}

func createEnc(files []*os.File) []*json.Encoder {
    var tempEnc *json.Encoder
    var result []*json.Encoder

    for _, file := range files {
        tempEnc = json.NewEncoder(file)
        result = append(result, tempEnc)
    }
    return result
}

// Start map task
func (worker *Worker) StartMap(args *MapStartSend, _ ArgEmpty) error {

    content, err := ioutil.ReadFile(args.InputFile)
    if err != nil {
        log.Fatal("Cannot Open Map Source")
    }

    tempFiles := createTemps(args.ReduceNum)
    encoders := createEnc(tempFiles)

    go func() {
        for _, kv := range worker.fMap(args.InputFile, string(content)) {
            id := iHash(kv.Key) % args.ReduceNum
            if encoders[id].Encode(&kv) != nil {
                log.Fatal("Unable To Encode Map Result")
            }
        }

        send := TaskFinishedSend{TaskId: args.TaskId, TaskType: MAP, WorkerId: worker.port}
        result := GeneralReply{}

        Call(worker.masterPort, "Master.TaskFinished", &send, &result)

        if result.Err == OK {
            for i := 0; i < args.ReduceNum; i++ {
                name := tempFiles[i].Name()
                tempFiles[i].Close()
                os.Rename(
                    name,
                    "mapresult/"+IRP+"-"+int2str(int(args.TaskId))+"-"+int2str(i),
                )
            }
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
