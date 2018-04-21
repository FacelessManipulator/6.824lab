package mapreduce

import (
	"fmt"
	"sync/atomic"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	waitAndDone := func(wa string, t DoTaskArgs, done *int32) {
		call(wa, "Worker.DoTask", t, nil)
		atomic.AddInt32(done, 1)
		registerChan<-wa
	}

	// generate task sequence
	tasks := make([]DoTaskArgs, 0, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks = append(tasks, DoTaskArgs{jobName, mapFiles[i], phase, i, n_other})
	}
	var done int32
	for done = 0; len(tasks) > 0; {
		if workerAddress, ok := <-registerChan; ok {
			task := tasks[0]
			tasks = tasks[1:]
			go waitAndDone(workerAddress, task, &done)
		} else {
			fmt.Printf("registerChan closed too early\n")
		}
	}
	for ; done < int32(ntasks); {time.Sleep(1000 * time.Millisecond)}
	fmt.Printf("Schedule: %v done\n", phase)
}
