package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	nmaps         int
	nreduce       int
	maptasks      []MapReduceTask
	rtasks        []MapReduceTask
	allmapdone    bool
	allreducedone bool
	maptaskIndex  int
	rtaskIndex    int
	inputfiles    []string
	mu            sync.Mutex
}

const intermediate_file_prefix = "mr"
const final_out_file_prefix = "mr-out"

func CheckIfTaskExpired(Task MapReduceTask) bool {
	if time.Since(Task.Starttime) > 10*time.Second {
		fmt.Printf("Task ID=%d timedout. Reallocating\n", Task.Tnumber)
		return true
	}
	return false
}

func FindFirstUnfinishedTask(TaskList []MapReduceTask) int {
	for i, task := range TaskList {
		if task.Tstate == Assigned {
			if CheckIfTaskExpired(task) {
				return i
			} else {
				continue
			}
		}
	}
	return -1
}

func AllocateNewTask(c *Coordinator, args *RequestTaskArgs,
	reply *RequestTaskReply) error {
	var s TaskType
	var taskList []MapReduceTask
	var currentTaskIndex int

	// Take lock now otherwise same task can be assigned to same RPC
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.allmapdone {
		s = Reduce
		taskList = c.rtasks
		currentTaskIndex = c.rtaskIndex
	} else {
		s = Map
		taskList = c.maptasks
		currentTaskIndex = c.maptaskIndex
	}

	if currentTaskIndex < len(taskList) {
		// Assign a new task
		task := &taskList[currentTaskIndex]
		task.Starttime = time.Now()
		task.Tstate = Assigned
		reply.Task = *task
		if s == Map {
			c.maptaskIndex++
		} else {
			c.rtaskIndex++
		}
		return nil
	}
	ti := FindFirstUnfinishedTask(taskList)
	if ti != -1 {
		taskList[ti].Starttime = time.Now()
		reply.Task = taskList[ti]
		return nil
	}
	// All Tasks are Assigned but not timed-out and waiting for completion
	// Send Wait task
	task := MapReduceTask{
		Ttype: Wait,
	}
	reply.Task = task
	return nil
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *RequestTaskArgs,
	reply *RequestTaskReply) error {
	//workerid := args.WorkerId
	// fmt.Printf("Received Request Task from worker id=%s\n", workerid)
	if !c.allmapdone || !c.allreducedone {
		ret := AllocateNewTask(c, args, reply)
		//fmt.Printf("worker got Task =%v \n", reply.Task)
		return ret
	}
	// All tasks are done. Tell worker to exit or do something else
	t := MapReduceTask{
		Ttype: Exit,
	}
	reply.Task = t
	return nil
}

func checkifAllTasksDone(c *Coordinator, t TaskType) {
	if t == TaskType(Map) {
		for _, task := range c.maptasks {
			if task.Tstate != Finished {
				return
			}
		}
		c.allmapdone = true
	} else {
		for _, task := range c.rtasks {
			if task.Tstate != Finished {
				return
			}
		}
		c.allreducedone = true
		if c.allmapdone && c.allreducedone {
			fmt.Printf("All tasks are done now %v", time.Now())
		}
	}
}

func (c *Coordinator) NotifyComplete(args *NotifyCompleteArgs,
	reply *NotifyCompleteReply) error {
	taskid := args.Tnumber
	tasktype := args.TType

	if tasktype == TaskType(Map) {
		if taskid < c.nmaps {
			c.maptasks[taskid].Tstate = Finished
		} else {
			return errors.New("invalid task number was passed for a map task")
		}
	} else if tasktype == TaskType(Reduce) {
		if taskid < c.nreduce {
			c.rtasks[taskid].Tstate = Finished
		} else {
			return errors.New("invalid task number was passed for a reduce task")
		}
	} else {
		fmt.Printf("Invalid Task Completion request sent")
		return errors.New("invalid Task type was passed")
	}
	checkifAllTasksDone(c, tasktype)
	// * All done on coordinator to complete Notify Complete
	reply.Returncode = 0
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.allmapdone && c.allreducedone {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nmaps:         len(files),
		nreduce:       nReduce,
		maptasks:      make([]MapReduceTask, len(files)),
		rtasks:        make([]MapReduceTask, nReduce),
		allmapdone:    false,
		allreducedone: false,
		maptaskIndex:  0,
		rtaskIndex:    0,
		inputfiles:    files,
		mu:            sync.Mutex{},
	}

	// Initialize Map tasks
	for i := range c.maptasks {
		c.maptasks[i] = MapReduceTask{
			Ttype:     Map,
			Tstate:    Unassigned,
			Tnumber:   i,
			WorkerId:  "",
			Starttime: time.Now(),
			Nreduce:   nReduce,
			// In current design input files for each map task is
			// partiotioned as single input file as nmaps = num of files
			// ToDO: better partitioning method to divide inout files
			InputFiles:  []string{files[i]},
			OutputFiles: create_map_task_intermediate_files(i, c.nreduce),
		}
	}

	reduceOpFiles := reduce_task_op_files(c.nreduce)

	for i := range c.rtasks {
		c.rtasks[i] = MapReduceTask{
			Ttype:       Reduce,
			Tstate:      Unassigned,
			Tnumber:     i,
			WorkerId:    "",
			Starttime:   time.Now(),
			InputFiles:  reduce_taskj_input_files(i, c.nmaps),
			OutputFiles: reduceOpFiles,
		}
	}

	c.server()
	return &c
}

// map task i will create imtermediate files in the format
// mr-out-i-j where j = [ 0, 1, ... nreduce ]
// map tasks should write key k to file (hash(k) % nReduce)
// IMP: All maps tasks should write same key to the same intermediate file.
func create_map_task_intermediate_files(maptaskid int, nReduce int) []string {
	var ret []string
	for i := 0; i < nReduce; i++ {
		ret = append(ret, fmt.Sprintf("%s-%d-%d", intermediate_file_prefix, maptaskid, i))
	}
	return ret
}

// Reduce task j works on all files mr-out-($maptasks)-j
// mapstasks = (0, 1, ..., nmaptasks)
// reduce task would find all keys(> 1) and their values in this file
func reduce_taskj_input_files(reducetaskid int, nMapnum int) []string {
	var ret []string
	for i := 0; i < nMapnum; i++ {
		ret = append(ret, fmt.Sprintf("%s-%d-%d", intermediate_file_prefix,
			i, reducetaskid))
	}
	return ret
}

func reduce_task_op_files(nReduce int) []string {
	var ret []string
	for i := 0; i < nReduce; i++ {
		ret = append(ret, fmt.Sprintf("%s-%d", final_out_file_prefix, i))
	}
	return ret
}
