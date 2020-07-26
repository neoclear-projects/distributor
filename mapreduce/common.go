// Copyright 2020 NeoClear. All rights reserved.
// Utils that are common to both master and worker

package mapreduce

import (
    "fmt"
    "hash/fnv"
    "log"
    "net"
    "net/rpc"
    "strconv"
    "time"
)

type Err string

const DURATION = time.Millisecond * 50
const OFFLINE = time.Millisecond * 500

const IRP = "mr"
const ROP = "wc"

const (
    OK   = "OK"
    FAIL = "FAIL"
)

// The placeholder for rpc function parameter
type ArgEmpty *struct{}

// The general reply object
// Carries a single Error object (aka string)
type GeneralReply struct {
    Err Err
}

// Key Value pair
// The intermediate value of map function
type KeyValue struct {
    Key   string
    Value string
}

// The function used to call rpc
func Call(port int64, rpcName string,
    args interface{}, reply interface{}) bool {
    // Get connection object
    client, err := rpc.Dial("tcp", ":"+strconv.FormatInt(port, 10))
    if err != nil {
        fmt.Println("Call:", err)
        return false
    }
    defer client.Close()

    // Connect using connection object
    e := client.Call(rpcName, args, reply)
    if e != nil {
        fmt.Println("Call:", e)
        return false
    }

    return true
}

func CreateServer(remoteObj interface{}, port int64,
    serverName string) (*rpc.Server, net.Listener) {
    rp := rpc.NewServer()
    rp.Register(remoteObj)

    listener, err := net.Listen("tcp", ":"+strconv.FormatInt(port, 10))
    if err != nil {
        log.Fatal(serverName, "listen error:", err)
    }

    return rp, listener
}

// The event loop that constantly deal with requests
func RunServer(serviceName string, server *rpc.Server, listener net.Listener) {
    for {
        conn, err := listener.Accept()
        if err == nil {
            go server.ServeConn(conn)
        } else {
            fmt.Println(serviceName, "done")
            break
        }
    }
    listener.Close()
}

// A function blocks duration
func Pause() {
    time.Sleep(DURATION)
}

// A function that blocks until f is true
func WaitUntil(f func() bool) {
    for !f() {
        Pause()
    }
}

// The hash function used to split map result
func iHash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}

// Convert int64 to string
func int2str(val int) string {
    return strconv.FormatInt(int64(val), 10)
}
