package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTasks    []Task
	ReduceTasks []Task
	Mrinfo      MRInfo
	mtx         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) IsMapFinished() bool {
	ret := true
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, task := range c.MapTasks {
		if task.TaskType == MapTask {
			if task.TaskStatus != Finished {
				ret = false
			}
		}
	}

	return ret
}

func (c *Coordinator) IsReduceFinished() bool {
	ret := true
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, task := range c.ReduceTasks {
		if task.TaskType == ReduceTask {
			if task.TaskStatus != Finished {
				ret = false
			}
		}
	}

	return ret
}

// Can be Map task or Reduce task
func (c *Coordinator) GetUnassignedTask() Task {
	c.mtx.Lock()
	// Read MapTasks first, if all the map tasks are completed, then read ReduceTasks
	for i, task := range c.MapTasks {
		if task.TaskStatus == UnAssigned {
			c.MapTasks[i].TaskStatus = Assigned
			c.mtx.Unlock()
			return task
		}
	}
	c.mtx.Unlock()

	if c.IsMapFinished() {
		c.mtx.Lock()
		for i, task := range c.ReduceTasks {
			if task.TaskStatus == UnAssigned {
				c.ReduceTasks[i].TaskStatus = Assigned
				c.mtx.Unlock()
				return task
			}
		}
		c.mtx.Unlock()
	}

	return Task{TaskType: NoMoreTask}
}

// Assign a task to a worker
func (c *Coordinator) AssignTask(args *ArgsType, reply *Task) error {
	*reply = c.GetUnassignedTask()

	if reply.TaskType == NoMoreTask {
		return errors.New("NoMoreMapTaskOrReduceTask")
		//return nil
	} else {
		timer := time.NewTimer(10 * time.Second)
		// 10s 之后将没有完成的task重置为UnAssigned并重新参与task分配
		go func(c *Coordinator, task *Task) {
			<-timer.C
			c.mtx.Lock()
			defer c.mtx.Unlock()
			if reply.TaskType == MapTask && c.MapTasks[reply.TaskIndex].TaskStatus == Assigned {
				c.MapTasks[reply.TaskIndex].TaskStatus = UnAssigned
			} else if reply.TaskType == ReduceTask && c.ReduceTasks[reply.TaskIndex].TaskStatus == Assigned {
				c.ReduceTasks[reply.TaskIndex].TaskStatus = UnAssigned
			}
		}(c, reply)

		return nil
	}
}

// Update task status to Finished
func (c *Coordinator) UpdateTaskStatus(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if args.TaskType == MapTask {
		for i, task := range c.MapTasks {
			if task.TaskIndex == args.TaskId {
				c.MapTasks[i].TaskStatus = Finished
				break
			}
		}
	} else if args.TaskType == ReduceTask {
		for i, task := range c.ReduceTasks {
			if task.TaskIndex == args.TaskId {
				c.ReduceTasks[i].TaskStatus = Finished
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) GetMRInfo(args ArgsType, reply *MRInfo) error {
	reply.MapNum = c.Mrinfo.MapNum
	reply.ReduceNum = c.Mrinfo.ReduceNum

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for !(c.IsMapFinished() && c.IsReduceFinished()) {
		time.Sleep(time.Second * 1)
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Mrinfo.MapNum = len(files)
	c.Mrinfo.ReduceNum = nReduce

	// Add Map tasks
	c.mtx.Lock()
	for i, filename := range files {
		task := Task{MapTask, filename, i, UnAssigned}
		c.MapTasks = append(c.MapTasks, task)
	}

	// Add Reduce tasks
	for i := 0; i < nReduce; i++ {
		task := Task{ReduceTask, "", i, UnAssigned}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}
	c.mtx.Unlock()

	c.server()
	return &c
}
