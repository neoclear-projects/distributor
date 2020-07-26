package main

import (
	"../mapreduce"
	"math/rand"
	"strings"
	"time"
)

func mapFunc(key, value string) []mapreduce.KeyValue {
	var kv []mapreduce.KeyValue
	for _, w := range strings.Split(value, " ") {
		kv = append(kv, mapreduce.KeyValue{Key: w, Value: "1"})
	}
	return kv
}

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
	rand.Seed(int64(time.Now().Second()))
	var PORT int64 = int64(rand.Int() % 1000 + 4000)


	master := mapreduce.MakeMaster(files, 3, mapFunc, mockReduce)
	master.RunServer(PORT)

	w1 := mapreduce.MakeWorker(PORT - 1000, PORT, mapFunc, mockReduce)
	w1.StartWorker()
	w2 := mapreduce.MakeWorker(PORT - 1100, PORT, mapFunc, mockReduce)
	w2.StartWorker()
	w3 := mapreduce.MakeWorker(PORT - 1200, PORT, mapFunc, mockReduce)
	w3.StartWorker()

	for !master.Done() {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
