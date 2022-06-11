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

const TASK_QUEUEING int = 0
const TASK_PROCESSING int = 1
const TASK_PROCESSED int = 2
const MAX_PROCESSING_TIME int64 = 12

type Coordinator struct {
	// Your definitions here.
	mapWorkState       []WorkState
	mapWorkFinishCount int

	reduceWorkState       []WorkState
	reduceWorkFinishCount int

	reduceCount int
	lock        sync.Mutex
}

type WorkState struct {
	workerId   string
	workState  int
	startTime  int64
	latestTime int64
	inputPath  []string
	outputPath []string
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

func (c *Coordinator) RequestWork(req *WorkerRequest, resp *WorkResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch req.ReqType {
	case IDLE:
		c.handleHeartBeatMsg(req, resp)
		break
	case BUSY:
		c.handleRunningMsg(req, resp)
		break
	case FINISH:
		c.handleFinishMsg(req, resp)
		break
	default:
		resp.WorkType = HOLD
	}
	return nil
}

func (c *Coordinator) handleHeartBeatMsg(req *WorkerRequest, resp *WorkResponse) error {
	if c.mapWorkFinishCount < len(c.mapWorkState) {
		// try allocate map work
		handleWorkState(req, resp, c.mapWorkState, MAP, c.reduceCount)
	} else if c.reduceWorkFinishCount < c.reduceCount {
		// try allocate reduce work
		handleWorkState(req, resp, c.reduceWorkState, REDUCE, c.reduceCount)
	} else {
		// stop worker
		resp.WorkType = STOP
	}
	return nil
}

func (c *Coordinator) handleRunningMsg(req *WorkerRequest, resp *WorkResponse) error {
	if c.mapWorkFinishCount < len(c.mapWorkState) {
		if req.WorkType != MAP {
			resp.WorkType = CANCEL
		} else {
			var work = c.mapWorkState[req.WorkId]
			work.latestTime = time.Now().Unix()
			resp.WorkType = HOLD
		}
	} else if c.reduceWorkFinishCount < c.reduceCount {
		if req.WorkType != REDUCE {
			resp.WorkType = CANCEL
		} else {
			var work = c.mapWorkState[req.WorkId]
			work.latestTime = time.Now().Unix()
			resp.WorkType = HOLD
		}
	} else {
		// stop worker
		resp.WorkType = STOP
	}
	return nil
}

func (c *Coordinator) handleFinishMsg(req *WorkerRequest, resp *WorkResponse) error {
	if c.mapWorkFinishCount < len(c.mapWorkState) {
		var work = &(c.mapWorkState[req.WorkId])
		if req.WorkType != MAP || work.workState == TASK_PROCESSED {
			// println(work.workerId + " " + req.WorkerId)
			resp.WorkType = CANCEL
		} else {
			work.latestTime = time.Now().Unix()
			// fmt.Printf("PROCESSED :%d", req.WorkId)
			work.workState = TASK_PROCESSED
			for index, _ := range c.reduceWorkState {
				rwork := &c.reduceWorkState[index]
				if req.FilePath[index] == "" {
					continue
				}
				rwork.inputPath = append(rwork.inputPath, req.FilePath[index])
			}
			resp.WorkType = CANCEL
			c.mapWorkFinishCount++
		}
	} else if c.reduceWorkFinishCount < c.reduceCount {
		var work = &(c.reduceWorkState[req.WorkId])
		if req.WorkType != REDUCE || work.workState == TASK_PROCESSED {
			resp.WorkType = CANCEL
		} else {
			work.latestTime = time.Now().Unix()
			work.workState = TASK_PROCESSED
			resp.WorkType = CANCEL
			c.reduceWorkFinishCount++
		}
	} else {
		// stop worker
		resp.WorkType = STOP
	}
	return nil
}

func handleWorkState(req *WorkerRequest, resp *WorkResponse, state []WorkState, stageWorkType int, reduceCount int) {
	for index, _ := range state {
		work := &state[index]
		if work.workState == TASK_QUEUEING || (work.workState == TASK_PROCESSING && time.Now().Unix()-work.startTime > MAX_PROCESSING_TIME) {
			// try allocate map work
			resp.WorkType = stageWorkType
			resp.WorkId = index
			resp.FilePath = work.inputPath
			resp.ReduceCount = reduceCount

			if work.workerId != "" {
				fmt.Printf("issue work %d from %v to %v\n", stageWorkType, work.workerId, req.WorkerId)
			}
			work.workerId = req.WorkerId
			work.workState = TASK_PROCESSING
			work.startTime = time.Now().Unix()
			work.latestTime = time.Now().Unix()
			return
		} else if work.workState == TASK_PROCESSING && work.workerId == req.WorkerId {
			work.latestTime = time.Now().Unix()
		}
	}
	resp.WorkType = HOLD
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
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	if c.reduceWorkFinishCount == c.reduceCount {
		time.Sleep(time.Duration(2) * time.Second)
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.reduceCount = nReduce
	var size = len(files)
	c.mapWorkState = make([]WorkState, size)
	for index, _ := range c.mapWorkState {
		c.mapWorkState[index] = WorkState{"", 0, 0, 0, []string{files[index]}, make([]string, 0)}
	}

	c.reduceWorkState = make([]WorkState, nReduce)
	for index, _ := range c.reduceWorkState {
		c.reduceWorkState[index] = WorkState{"", 0, 0, 0, make([]string, 0), make([]string, 0)}
	}

	c.server()
	return &c
}
