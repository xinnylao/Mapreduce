package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Empty struct {
	/**/
}
type MapTask struct {
	FileName      string
	MapTaskNumber int
}
type MapResult struct {
	MapTask  MapTask
	IntFiles []string
}
type ReduceTask struct {
	Bucket           []string
	ReduceTaskNumber int
}
type ReplyTask struct {
	TaskType   string
	MapTask    MapTask
	ReduceTask ReduceTask
	NReduce    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
