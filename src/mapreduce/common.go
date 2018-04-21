package mapreduce

import (
	"fmt"
	"strconv"
	"strings"
	"errors"
	"bytes"
	"io"
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

func encode(kv KeyValue) []byte{
	if strings.ContainsRune(kv.Key, 0) ||
		strings.ContainsRune(kv.Value, 0) {
			panic(errors.New("Assume KeyValue did not contains \\x00"))
	}
	return []byte(kv.Key + "\x00" + kv.Value + "\x00")
}

func decode(blob []byte, kv chan KeyValue){
	defer close(kv)
	buf := bytes.NewBuffer(blob)
	for {
		key, _ := buf.ReadBytes(0)
		value, err := buf.ReadBytes(0)
		if err == io.EOF {
			break
		} else {
			kv <- KeyValue{string(key[:len(key)-1]), string(value[:len(value)-1])}
		}
	}
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}
