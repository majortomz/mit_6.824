package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	HOLD   int = 0
	CANCEL int = 1
	MAP    int = 2
	REDUCE int = 3
	STOP   int = 4
)

const (
	IDLE   int = 1
	BUSY   int = 2
	FINISH int = 3
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerRequest struct {
	WorkerId string
	ReqType  int // 1 idle  2 busy  3 finish
	WorkType int
	WorkId   int
	FilePath []string
}

type WorkResponse struct {
	FilePath    []string
	WorkType    int // 0 hold, 1 cancel, 2 map, 3 reduce
	WorkId      int
	ReduceCount int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
