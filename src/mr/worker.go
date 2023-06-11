package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallAskTask()
		log.Printf("Worker: receive coordinator's reply %v \n", reply)
		switch reply.State {
		case MapJob:
			DoMapTask(mapf, reply)
			break
		case ReduceJob:
			DoReduceTask(reducef, reply)
			break
		case WaitJob:
			time.Sleep(5 * time.Second)
			break
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", reply.State))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Coordinator.AskTask", &args, &reply)
	return &reply
}

func DoMapTask(mapf func(string, string) []KeyValue, reply *TaskInfo) {
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// prepare output files and encoders
	nReduce := reply.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outindex := 0; outindex < nReduce; outindex++ {
		outFiles[outindex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	// distribute keys among mr-fileindex-*
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", reply.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	// store all kv pairs into temperate files
	fileprefix := "mr-" + strconv.Itoa(reply.FileIndex) + "-"
	for outIndex, file := range outFiles {
		outname := fileprefix + strconv.Itoa(outIndex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}

	// acknowledge master
	CallTaskDone(reply)

}

func DoReduceTask(reducef func(string, []string) string, reply *TaskInfo) {
	// read in all files as a kv array
	intermediate := []KeyValue{}
	prefix := "mr-"
	suffix := "-" + strconv.Itoa(reply.PartIndex)
	for idx := 0; idx < reply.NFiles; idx++ {
		inputName := prefix + strconv.Itoa(idx) + suffix
		file, err := os.Open(inputName)
		if err != nil {
			log.Fatalf("cannot open %v", inputName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.PartIndex)
	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", oname, err)
		panic("Create file error")
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(filepath.Join(ofile.Name()), oname)
	ofile.Close()
	CallTaskDone(reply)
}

// Tell the coordinator the task is completed
func CallTaskDone(task *TaskInfo) {
	//task.State = CompleteJob
	reply := ExampleReply{}
	call("Coordinator.TaskDone", &task, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
