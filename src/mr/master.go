package mr

import (
	//"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	status_map = "map"
	status_reduce = "reduce"
	status_exit = "exit"
	status_wait = "wait"
	next_status = map[string]string{
		status_map: status_reduce,
		status_reduce: status_exit,
		status_wait: status_wait,
		status_exit: status_exit,
	}
)


type Master struct {
	// Your definitions here.

	// variables for status control
	status string
	mu sync.Mutex
	// extra variables for map phase
	map_files []string // array of files in map phase
	nReduce int
	// variables for handling tasks
	num_total_tasks int // number of total files/successful map tasks needed to be performed
	remain_tasks map[int]bool // set of indexes of remaining tasks with no worker assigned
	finished_tasks map[int]bool // set of indexes of finished tasks 
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignWork(args *AssignWorkArgs, reply *AssignWorkReply) error {
	// reply workers with task information
	// 1. update current information if possible
	m.mu.Lock()
	num_tasks_finished := len(m.finished_tasks)
	if num_tasks_finished == m.num_total_tasks {
		m.status = next_status[m.status]
		if m.status == status_reduce {
			loadReduceInfo(m)
		}
	}
	// 2. reply with current status
	switch m.status {
	case status_map, status_reduce:
		if len(m.remain_tasks) == 0 { // empty working pool
			reply.TaskType = status_wait
		} else {
			reply.TaskType = m.status
			reply.NReduce = m.nReduce
			reply.NumMapFiles = len(m.map_files)
			reply.TaskIdx = get_some_key(m.remain_tasks)
			delete(m.remain_tasks, reply.TaskIdx)
			if m.status == status_map {
				reply.MapFile = m.map_files[reply.TaskIdx]
			}
			// after replying workers, start a goroutine to wait for its completeness
			defer func() {
				go waitWork(m, reply.TaskIdx)
			}()
		}
	case status_exit:
		reply.TaskType = status_exit
	}
	m.mu.Unlock()
	// 
	return nil
}

func (m *Master) FinishWork(args *AssignWorkReply, reply *AssignWorkArgs) error {
	task_idx := args.TaskIdx
	m.mu.Lock()
	m.finished_tasks[task_idx] = true
	m.mu.Unlock()
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	m.mu.Lock()
	num_tasks_finished := len(m.finished_tasks)
	ret := (m.status == status_reduce && num_tasks_finished == m.num_total_tasks) || m.status == status_exit
	m.mu.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.status = status_map
	m.nReduce = nReduce
	m.map_files, m.finished_tasks, m.remain_tasks = make([]string, 0), make(map[int]bool), make(map[int]bool)
	loadMapFiles(&m, &files)

	m.server()
	return &m
}


// load files for map phase for Master
func loadMapFiles(m *Master, files *[]string) {
	for index, filename := range *files {
		m.map_files = append(m.map_files, filename)
		m.remain_tasks[index] = true
	}
	m.num_total_tasks = len(m.map_files)
} 

func loadReduceInfo(m *Master) {
	m.status = status_reduce
	m.num_total_tasks = m.nReduce
	m.remain_tasks, m.finished_tasks = make(map[int]bool), make(map[int]bool)
	for i := 0; i < m.nReduce; i++ {
		m.remain_tasks[i] = true
	} 
}

func waitWork(m *Master, TaskIdx int) {
	seconds := 10
	time.Sleep(time.Duration(seconds) *time.Second)
	m.mu.Lock()
	_, ok := m.finished_tasks[TaskIdx]
	switch {
	case ok: // the task has been done successfully! In this case, nothing to perform
	default: // Otherwise, add task back into working pool
		m.remain_tasks[TaskIdx] = true
	}
	m.mu.Unlock()
}

func get_some_key(m map[int]bool) int {
    for k := range m {
        return k
    }
    return 0
}
