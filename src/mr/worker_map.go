package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func domap(mapf func(string, string) []KeyValue,
	task MapReduceTask) {
	input_files := task.InputFiles
	nreduce := task.Nreduce

	for _, filename := range input_files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate := make([][]KeyValue, nreduce)
		for _, kv := range kva {
			//TODO: Not able to pass nReduce from coordinator to worker
			r := ihash(kv.Key) % nreduce
			intermediate[r] = append(intermediate[r], kv)
		}

		for r, kva := range intermediate {
			ofname := fmt.Sprintf("%s-%d-%d", intermediate_file_prefix, task.Tnumber, r)
			tmpfile, _ := ioutil.TempFile("", ofname)
			enc := json.NewEncoder(tmpfile)
			for _, kv := range kva {
				enc.Encode(&kv)
			}
			tmpfile.Close()
			os.Rename(tmpfile.Name(), ofname)
		}
	}
	rc := CallNotifyCompleteRPC(task.Ttype, task.Tnumber)
	if rc != 0 {
		fmt.Printf("Failed to ack notify complete to Coordinator. Retry")
	}
}
