package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapJob = iota
	ReduceJob
	WaitJob
	CompleteJob
)

type Coordinator struct {
	// Your definitions here.
	filenames []string
	nReduce   int

	mapTaskWaiting    TaskStatQueue
	mapTaskRunning    TaskStatQueue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	isDone            bool
	mapDistributed    bool
	mapDone           bool
	reduceDistributed bool
	reduceDone        bool
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mu        sync.Mutex
}

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type TaskInfo struct {
	State     int // declared in const above
	FileName  string
	FileIndex int
	PartIndex int
	NReduce   int
	NFiles    int
}

type TaskStat struct {
	beginTime time.Time
	fileName  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

type MapTaskStat struct {
	TaskStat
}

func (this *TaskStatQueue) Lock() {
	this.mu.Lock()
}

func (this *TaskStatQueue) Unlock() {
	this.mu.Unlock()
}

func (this *MapTaskStat) OutOfTime() bool {
	// return true when run out of time
	return time.Since(this.beginTime).Seconds() > 10
}

func (this *MapTaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *MapTaskStat) GetPartIndex() int {
	return this.partIndex
}

func (this *MapTaskStat) SetNow() {
	this.beginTime = time.Now()
}

type ReduceTaskStat struct {
	TaskStat
}

func (this *ReduceTaskStat) OutOfTime() bool {
	// return true when run out of time
	return time.Since(this.beginTime).Seconds() > 10
}

func (this *ReduceTaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *ReduceTaskStat) GetPartIndex() int {
	return this.partIndex
}

func (this *ReduceTaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     MapJob,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     ReduceJob,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *TaskStatQueue) Size() int {
	//this.Lock()
	//defer this.Unlock()
	return len(this.taskArray)
}

func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.Lock()
	//defer this.Unlock()
	length := this.Size()
	fmt.Printf("Current size is %v\n", this.Size())
	if length != 0 {
		task := this.taskArray[length-1]
		this.taskArray = this.taskArray[:length-1]
		//fmt.Printf("Tasks left are %v\n", length-1)
		this.Unlock()
		return task
	}
	// no task left
	this.Unlock()
	return nil
}

func (this *TaskStatQueue) Push(task TaskStatInterface) {
	this.Lock()
	//defer this.Unlock()
	this.taskArray = append(this.taskArray, task)
	this.Unlock()
	return
}

func (this *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		// record task start time
		mapTask.SetNow()
		// note the task is running
		this.mapTaskRunning.Push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distribute map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// when all map tasks are finished, distribute reduce tasks
	if this.mapTaskWaiting.Size() == 0 && this.mapTaskRunning.Size() == 0 {
		this.mapDone = true
	}
	if this.mapDone && !this.reduceDistributed {
		this.DistributeReduce()
	}

	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// record task start time
		reduceTask.SetNow()
		// note the task is running
		this.reduceTaskRunning.Push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		//fmt.Printf("Reduce task distributed on %v th file\n", reduceTask.GetFileIndex())
		fmt.Printf("Distribute reduce task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// check if any task still running
	if this.mapTaskRunning.Size() != 0 || this.reduceTaskRunning.Size() != 0 {
		reply.State = WaitJob
		return nil
	}

	// all tasks completed
	reply.State = CompleteJob
	this.reduceDone = true
	this.isDone = true
	return nil
}

func (this *Coordinator) DistributeMap() {
	for idx, file := range this.filenames {
		fmt.Printf("Prepare map task on file %v %v\n", idx, file)
		this.mapTaskWaiting.Push(&MapTaskStat{TaskStat{
			time.Now(),
			file,
			idx,
			-1,
			this.nReduce,
			len(this.filenames),
		},
		})
	}
	this.mapDistributed = true
}

func (this *Coordinator) DistributeReduce() {
	//if this.reduceDistributed == true {
	//	return
	//}
	for i := 0; i < this.nReduce; i++ {
		fmt.Printf("Prepare reduce task on part %v\n", i)
		this.reduceTaskWaiting.Push(&ReduceTaskStat{TaskStat{
			time.Now(),
			"",
			-1,
			i,
			this.nReduce,
			len(this.filenames),
		}})
	}
	this.reduceDistributed = true
}

// Your code here -- RPC handlers for the worker to call.
// Take ask args and reply args.
// Respond with the file name of an as-yet-unstarted map task.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (this *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (this *Coordinator) server() {
	rpc.Register(this)
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

func (this *Coordinator) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case MapJob:
		fmt.Printf("Map task done on %vth file %v\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	case ReduceJob:
		fmt.Printf("Reduce task done on %vth file %v\n", args.FileIndex, args.FileName)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done Error")
	}
	return nil
}

func (taskqueue *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	// for map task, no part index, part index = -1
	if partIndex == -1 {
		newTaskArray := []TaskStatInterface{}
		for _, task := range taskqueue.taskArray {
			if task.GetFileIndex() != fileIndex {
				newTaskArray = append(newTaskArray, task)
			}
		}
		taskqueue.taskArray = newTaskArray
	} else { // for reduce task
		newTaskArray := []TaskStatInterface{}
		for _, task := range taskqueue.taskArray {
			if task.GetPartIndex() != partIndex {
				newTaskArray = append(newTaskArray, task)
			}
		}
		taskqueue.taskArray = newTaskArray
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (this *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	ret := this.isDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	this := Coordinator{}

	// Your code here.
	this.initCoordinator(files, nReduce)

	this.server()
	return &this
}

func (this *Coordinator) initCoordinator(files []string, nReduce int) {
	this.isDone = false
	this.filenames = files
	this.nReduce = nReduce
	this.reduceDistributed = false
	this.mapTaskRunning = TaskStatQueue{}
	this.mapTaskWaiting = TaskStatQueue{}
	this.reduceTaskRunning = TaskStatQueue{}
	this.reduceTaskWaiting = TaskStatQueue{}

	this.DistributeMap()
}
