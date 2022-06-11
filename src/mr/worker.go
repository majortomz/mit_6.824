package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type WorkerImpl struct {
	workerId string
	state    int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string

	reduceCount int
	workType    int
	workId      int
	inputPath   []string
	outputPath  []string

	waitGroup sync.WaitGroup
}

//
// Map functions return a slice of KeyValue.
//
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
	workerNo := strconv.Itoa(os.Getpid())
	worker := WorkerImpl{workerId: workerNo, state: IDLE}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.waitGroup.Add(1)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	worker.sendMsgToCoordinator()
}

func (w *WorkerImpl) sendMsgToCoordinator() {
	for {
		time.Sleep(time.Duration(1) * time.Second)
		req := WorkerRequest{}
		req.WorkerId = w.workerId
		req.ReqType = w.state
		req.WorkType = w.workType
		req.WorkId = w.workId
		req.FilePath = w.outputPath

		resp := WorkResponse{}
		ok := call("Coordinator.RequestWork", &req, &resp)
		if ok {
			//fmt.Printf("%v", resp)
			if resp.WorkType == STOP {
				return
			}
			w.handleResp(&resp)
		} else {
			fmt.Printf(w.workerId + "call failed!\n")
		}
	}
}

func (w *WorkerImpl) handleResp(resp *WorkResponse) {
	if resp.WorkType == CANCEL {
		w.state = IDLE
		w.workType = 0
		w.workId = 0
		w.inputPath = nil
		w.outputPath = nil
		w.reduceCount = 0
	} else if resp.WorkType == MAP {
		w.inputPath = resp.FilePath
		w.workId = resp.WorkId
		w.workType = resp.WorkType
		w.reduceCount = resp.ReduceCount
		w.state = BUSY

		intermediate := make([][]KeyValue, w.reduceCount)
		for _, filename := range w.inputPath {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := w.mapf(filename, string(content))
			for _, kv := range kva {
				idx := ihash(kv.Key) % w.reduceCount
				intermediate[idx] = append(intermediate[idx], kv)
			}
		}
		for _, kvs := range intermediate {
			sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		}

		for idx := 0; idx < w.reduceCount; idx++ {
			reduceO := intermediate[idx]
			if reduceO == nil || len(reduceO) <= 0 {
				w.outputPath = append(w.outputPath, "")
				continue
			}
			filename := "mr-" + strconv.Itoa(w.workId) + "-" + strconv.Itoa(idx)
			file, err := ioutil.TempFile("", filename)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			for _, kv := range reduceO {
				fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
			}
			file.Close()
			os.Rename(file.Name(), filename)
			w.outputPath = append(w.outputPath, filename)
		}
		w.state = FINISH
	} else if resp.WorkType == REDUCE {
		w.inputPath = resp.FilePath
		w.workId = resp.WorkId
		w.workType = resp.WorkType
		w.reduceCount = resp.ReduceCount
		w.state = BUSY

		kvs := []KeyValue{}
		for _, filename := range w.inputPath {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				kv := strings.Split(line, " ")
				if len(kv) < 2 {
					continue
				}
				kvs = append(kvs, KeyValue{kv[0], kv[1]})
			}
		}

		sort.Sort(ByKey(kvs))
		filename := "mr-out-" + strconv.Itoa(w.workId)
		ofile, _ := ioutil.TempFile("", filename)
		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := w.reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
			i = j
		}
		os.Rename(ofile.Name(), filename)
		w.state = FINISH
	} else if resp.WorkType == STOP {
		return
	}
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
