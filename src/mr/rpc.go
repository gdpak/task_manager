package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType int

const (
	Exit TaskType = iota
	Wait
	Map
	Reduce
)

type State int

const (
	Unassigned State = iota
	Assigned
	Finished
)

type MapReduceTask struct {
	Ttype       TaskType
	Tstate      State
	Tnumber     int
	WorkerId    string
	Nreduce     int
	InputFiles  []string
	OutputFiles []string
	Starttime   time.Time
}

type RequestTaskArgs struct {
	WorkerId string
}

type RequestTaskReply struct {
	Task MapReduceTask
}

type NotifyCompleteArgs struct {
	TType   TaskType
	Tnumber int
}

type NotifyCompleteReply struct {
	Returncode int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func (t TaskType) String() string {
	switch t {
	case Exit:
		return "Exit"
	case Wait:
		return "Wait"
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	default:
		return "Invalid Task Type"
	}
}
