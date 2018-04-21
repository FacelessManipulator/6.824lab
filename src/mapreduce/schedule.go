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

	// generate task channel
	// the better way is make a fix size channel and push task asyncronously
	tasks := make(chan DoTaskArgs, 16)
	defer close(tasks)
	go func() {
		for i := 0; i < ntasks; i++ {
			tasks <- DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		}
	}()

	// handle worker registe
	timeout := time.NewTimer(time.Second)
	for done := int32(0); done < int32(ntasks); {
		timeout.Reset(time.Second)
		select{
		case workerAddress := <-registerChan:
			go func() {
				for ok := true; ok; {
					if task, ok := <- tasks; ok {
						if call(workerAddress, "Worker.DoTask", task, nil) {
							atomic.AddInt32(&done, 1)
						} else {
							tasks <- task
						}
					}
				}
			}()
		case <-timeout.C:
		}
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
