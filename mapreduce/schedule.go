package mapreduce

import (
	"time"
)

func schedule(master *Master) {
	// Run thread to periodically check available workers to assign tasks
	go master.checkAvailableWorkerForTask(MAP)

	// Run thread to periodically remove unavailable worker
	go master.removeUnavailableWorker(MAP)
	// Wait for map to be finished
	for master.MapFinished() {
		time.Sleep(time.Second)
	}

	go master.checkAvailableWorkerForTask(REDUCE)
	go master.removeUnavailableWorker(REDUCE)

	// Wait for reduce to be finished
	for master.ReduceFinished() {
		time.Sleep(time.Millisecond)
	}
}
