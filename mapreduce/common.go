// Copyright 2020 NeoClear. All rights reserved.
// Utils that are common to both master and worker

package mapreduce

import (
    "fmt"
    "log"
    "net"
    "net/rpc"
    "strconv"
    "time"
)

type Err string

const DURATION = time.Millisecond * 50

const (
    OK   = "OK"
    FAIL = "FAIL"
)

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
