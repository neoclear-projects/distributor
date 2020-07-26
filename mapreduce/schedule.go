package mapreduce

// Function that control the workflow of distributor
// Finish map task, then goes to reduce task
func schedule(master *Master) {
    // Run thread to periodically check available workers to assign tasks
    go master.checkAvailableWorkerForTask(MAP)

    // Run thread to periodically remove unavailable worker
    go master.removeUnavailableWorker(MAP)

    // Wait for map to be finished
    WaitUntil(master.MapFinished)

    // Run thread to periodically check available workers to assign reduce tasks
    go master.checkAvailableWorkerForTask(REDUCE)

    // Run thread to periodically remote unavailable worker
    go master.removeUnavailableWorker(REDUCE)

    // Wait for reduce to be finished
    WaitUntil(master.ReduceFinished)
}
