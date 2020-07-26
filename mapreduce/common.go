package mapreduce

import (
	"fmt"
	"net/rpc"
	"strconv"
)

type Err string

const (
	OK = "OK"
	FAIL = "FAIL"
)

type GeneralReply struct {
	Err Err
}

type KeyValue struct {
	Key string
	Value string
}

func Call(port int64, rpcName string,
	args interface{}, reply interface{}) bool {
	client, err := rpc.Dial("tcp", ":"+strconv.FormatInt(port, 10))
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer client.Close()

	e := client.Call(rpcName, args, reply)
	if e != nil {
		fmt.Println(e)
		return false
	}
	return true
}
