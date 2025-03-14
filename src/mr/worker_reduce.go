package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doreduce(reducef func(string, []string) string,
	task MapReduceTask) {
	// Every reduce task j will have to look at all the files
	// created by different map tasks . mr-out-x-j where x = [1, 2 , ... nmaps]
	// These files are provided in task's input files
	intermediate := []KeyValue{}
	for m := 0; m < len(task.InputFiles); m++ {
		ifile := task.InputFiles[m]
		file, err := os.Open(ifile)
		if err != nil {
			log.Fatalf("Can not open %v", ifile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// intermediate now have [{Key=x, Value=1}, {Key=y, value=1}, {Key=x,
	// value=1}...]
	// Keys are sorted in the order they appear in individual intermediate
	// key files. Let us Sort and then collect all duplicates and run reduce on them
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// This reduce task will write to mr-out-(TaskNum)
	ofilename := fmt.Sprintf("%s-%d", final_out_file_prefix, task.Tnumber)
	ofile, _ := os.CreateTemp("", ofilename)

	// Lets run reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), ofilename)
	ok := CallNotifyCompleteRPC(task.Ttype, task.Tnumber)
	if ok != 0 {
		fmt.Printf("Could not mark complete task num %d", task.Tnumber)
	}

}
