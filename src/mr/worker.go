package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Lets generate a UUID for this worker instance
	id := uuid.New()
	// fmt.Printf("id generated is=%s\n", id.String())
	for {
		returnedTaskReply := CallRequestTask(id.String())
		returnedTask := returnedTaskReply.Task
		switch returnedTask.Ttype {
		case Exit:
			//fmt.Printf("Received Exit. Coorinator has nothing to offer. Exit ...")
			return
		case Map:
			//fmt.Printf("Map Received ")
			domap(mapf, returnedTask)
		case Reduce:
			//fmt.Printf("Reduce Received ")
			doreduce(reducef, returnedTask)
		case Wait:
			//fmt.Printf("Wait Received ")
			time.Sleep(2 * time.Second)
		}
		// Wait for 1 second before asking next task
		time.Sleep(1 * time.Second)
	}
}

// Call Request Task
func CallRequestTask(workerId string) RequestTaskReply {
	// declare an argument structure.
	args := RequestTaskArgs{
		WorkerId: workerId,
	}
	// declare a reply structure.
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call Request Task failed!\n")
	}
	//fmt.Printf("Task received is %v\n", reply.Task)
	return reply
}

// Call Notify Complete RPC to tell coordinator we are done
func CallNotifyCompleteRPC(ttype TaskType, tn int) int {
	args := NotifyCompleteArgs{
		TType:   ttype,
		Tnumber: tn,
	}
	reply := NotifyCompleteReply{0}
	ok := call("Coordinator.NotifyComplete", &args, &reply)
	if !ok {
		fmt.Printf("call Notify Complete call failed!\n")
	}
	return reply.Returncode

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
