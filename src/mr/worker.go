package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
	
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	wait_seconds = 1
	std_prefix = "mr"
)

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
	
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		// 1. ask for task from master
		task_info := getTask()
		// 2. handle tasks 
		switch task_info.TaskType {
		case status_map:
			apply_map(mapf, reducef, task_info)
		case status_reduce:
			apply_reduce(mapf, reducef, task_info)
		case status_exit:
			os.Exit(1)
		case status_wait:
			time.Sleep(time.Duration(wait_seconds) * time.Second)
		}
	}

}

func getTask() AssignWorkReply{
	args := AssignWorkArgs{}
	reply := AssignWorkReply{}
	if !call("Master.AssignWork", &args, &reply) {
		reply.TaskType = status_exit
	}
	return reply
}

func apply_map(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task_info AssignWorkReply) {
	//fmt.Printf("starts map!\n")
	// map contents
	filename := task_info.MapFile
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
	// create temp files to write
	processID := strconv.Itoa(os.Getpid())
	tempfiles := make([]*json.Encoder, task_info.NReduce)
	for i := 0; i < task_info.NReduce; i++ {
		tempfileName := tempFileName(processID, task_info.TaskIdx, i)
		tempfile, _ := os.OpenFile(tempfileName,  os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(tempfile)
		tempfiles[i] = enc
	}
	// write outputs of map to temp files
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % task_info.NReduce
		enc := tempfiles[reduceIndex]
		enc.Encode(&kv)
	}
	// change file names
	for i := 0; i < task_info.NReduce; i++ {
		temp := tempFileName(processID, task_info.TaskIdx, i)
		target := tempFileName(std_prefix, task_info.TaskIdx, i)
		changeFileName(temp, target)
	}
	// done! 
	replyDone(task_info)
}

func apply_reduce(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task_info AssignWorkReply) {

	kva := []KeyValue{}
	for i := 0; i < task_info.NumMapFiles; i++ {
		tempName := tempFileName(std_prefix, i, task_info.TaskIdx)
		readJsonFile(tempName, &kva)
	}
	processID := strconv.Itoa(os.Getpid())
	performReduce(&kva, reducef, task_info.TaskIdx)
	current := fmt.Sprintf("%s-out-%d", processID, task_info.TaskIdx)
	target := fmt.Sprintf("%s-out-%d", std_prefix, task_info.TaskIdx)
	changeFileName(current, target)
	replyDone(task_info)
}

func tempFileName(prefix string, mapTaskIndex int, reduceTaskIndex int) (res string) {
	res = fmt.Sprintf("%s-%d-%d", prefix, mapTaskIndex, reduceTaskIndex)
	return 
}

func replyDone(args AssignWorkReply) {
	reply := AssignWorkArgs{}
	call("Master.FinishWork", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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


func readJsonFile(filename string, kva *[]KeyValue) {
	file, _ := os.Open(filename)
	dec := json.NewDecoder(file)
	for {
	  var kv KeyValue
	  if err := dec.Decode(&kv); err != nil {
		break
	  }
	  *kva = append(*kva, kv)
	}
}

func performReduce(intermediate* []KeyValue, reducef func(string, []string) string, task_idx int) {
	sort.Sort(ByKey(*intermediate))
	processID := strconv.Itoa(os.Getpid())
	oname := fmt.Sprintf("%s-out-%d", processID, task_idx)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to temp files.
	//
	i := 0
	for i < len(*intermediate) {
		j := i + 1
		for j < len(*intermediate) && (*intermediate)[j].Key == (*intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (*intermediate)[k].Value)
		}
		output := reducef((*intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", (*intermediate)[i].Key, output)

		i = j
	}

	ofile.Close()
}

func changeFileName(current, target string) {
	if _, err := os.Stat(current); err == nil { //if current file exists
		os.Rename(current, target)
	} else { //current file doesn't exists, create target
		os.Create(target)
	}
}