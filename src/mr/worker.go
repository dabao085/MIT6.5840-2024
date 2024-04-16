package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Get the number of Reduce tasks from coordinator
	args := ArgsType{}
	var reply MRInfo
	ok := call("Coordinator.GetMRInfo", args, &reply)
	if !ok {
		log.Println("Fail to GetMRInfo, Worker func is going to  exist")
		return
	}

	// Loop until RPC connection is broken, it normally means that the coordinator has existed.
	for {
		// Step1. Get a task from coordinator, get Reduce task only after all the Map tasks have
		// been completed.
		task := GetTask(ArgsType{})

		// Step2. Execute this task and update status to coordinator
		if task.TaskType == MapTask {
			DoMapOperation(&task, reply.ReduceNum, mapf)
		} else if task.TaskType == ReduceTask {
			DoReduceOperation(&task, reply.MapNum, reducef)
		} else {
			log.Println("Worker: Unsupported task type")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func DoMapOperation(task *Task, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	kva := mapf(task.FileName, string(content))

	// split output by the hash result
	intermediate := make(map[int][]KeyValue)
	for _, line := range kva {
		index := ihash(line.Key) % nReduce
		intermediate[index] = append(intermediate[index], line)
	}

	// open output file and write with JSON format
	for reduceId, data := range intermediate {
		oname := "mr-" + strconv.Itoa(task.TaskIndex) + "-" + strconv.Itoa(reduceId)
		//log.Printf("Writing file:%v\n", oname)
		//ofile, _ := os.Create(oname)
		ofile, _ := ioutil.TempFile(".", "temp-")

		enc := json.NewEncoder(ofile)
		for _, kv := range data {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("enc.Encode failed, key=%v, value=%v\n", kv.Key, kv.Value)
			}
		}

		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}

	UpdateTaskStatus(task)
}

func DoReduceOperation(task *Task, nMap int, reducef func(string, []string) string) {
	var kva []KeyValue

	// read reduce input files with JSON format
	for i := 0; i < nMap; i++ {
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskIndex)
		//log.Printf("Reading file:%v\n", oname)
		ofile, _ := os.Open(oname)

		dec := json.NewDecoder(ofile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(task.TaskIndex)
	//ofile, _ := os.Create(oname)
	ofile, _ := ioutil.TempFile(".", "temp-")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)
	ofile.Close()

	UpdateTaskStatus(task)
}

func UpdateTaskStatus(task *Task) {
	args := TaskFinishArgs{}
	reply := TaskFinishReply{}
	args.TaskType = task.TaskType
	args.TaskId = task.TaskIndex
	ok := call("Coordinator.UpdateTaskStatus", &args, &reply)
	if !ok {
		if task.TaskType == MapTask {
			log.Printf("MapTask failed! fileName=%v, taskId=%v\n", task.FileName, task.TaskIndex)
		} else if task.TaskType == ReduceTask {
			log.Printf("ReduceTask failed! fileName=%v, taskId=%v\n", task.FileName, task.TaskIndex)
		}
	}
}

// Get a task from the coordinator
func GetTask(args ArgsType) Task {
	reply := Task{}

	for {
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {
			if reply.TaskType != NoMoreTask {
				break
			} else {
				log.Println("NoMoreTask")
			}
		}
		// Can not get a available task to execute, wait for a second and try to get again.
		time.Sleep(time.Second * 1)
	}

	log.Printf("Worker: Task.TaskType: %d Task.TaskIndex %d Task.TaskStatus %d\n",
		reply.TaskType, reply.TaskIndex, reply.TaskStatus)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
