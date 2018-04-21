package mapreduce

import (
	"encoding/json"
	// "bytes"
	"io/ioutil"
	// "io"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// inputs = make([nMap][]byte)
	kvs := make(map[string][]string)
	for m := 0; m < nMap; m++ {
		input, err := ioutil.ReadFile(reduceName(jobName, m, reduceTask))
		passOrPanic(err)
		// concentrate
		// dec := json.NewDecoder(bytes.NewReader(input))
		kvc := make(chan KeyValue)
		go decode(input, kvc)
		var kv KeyValue
		for ok := true; ok; {
			if kv, ok = <- kvc; ok {
				if v, ok2 := kvs[kv.Key]; ok2 {
					kvs[kv.Key] = append(v, kv.Value)
				} else {
					kvs[kv.Key] = []string{kv.Value}
				}
			}
			// var kv KeyValue
			// if err := dec.Decode(&kv); err == io.EOF {
			// 	break
			// } else if err != nil {
			// 	panic(err)
			// }

			// if v, ok := kvs[kv.Key]; ok {
			// 	kvs[kv.Key] = append(v, kv.Value)
			// } else {
			// 	kvs[kv.Key] = []string{kv.Value}
			// }
		}
	}

	// reduce and write output
	output, err := os.Create(outFile)
	defer output.Close()
	passOrPanic(err)
	for k, v := range kvs {
		red := reduceF(k ,v)
		b, _ := json.Marshal(KeyValue{k, red})
		output.Write(b)
	}
}
