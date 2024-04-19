package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// Task info for worker
const (
	MapTask int = iota
	ReduceTask
	NoMoreTask
)

// Task status for coordinator
const (
	UnAssigned int = iota
	Assigned
	Finished
)

type MRInfo struct {
	MapNum    int
	ReduceNum int
}

type ArgsType struct {
}

type Task struct {
	TaskType   int
	FileName   string // for Map tasks
	TaskIndex  int    // for both Map andReduce tasks
	TaskStatus int    // for coordinator
}

type TaskManager struct {
	Task  Task
	Timer *time.Timer
}

type TaskFinishArgs struct {
	TaskType int
	TaskId   int
}

type TaskFinishReply struct {
}

type EmptyRequest struct {
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
