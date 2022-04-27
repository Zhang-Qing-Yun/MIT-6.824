package mr

//
// RPC definitions.
//

import "os"
import "strconv"

type TaskType int

const(
	MapType TaskType = iota
	ReduceType
	WaitType
	ExitType
)

type GetTaskReq struct {
}

type GetTaskResp struct {
	Task     Task
	TypeOfTask TaskType
}

type DoneTaskReq struct {
	TaskId     int
	TypeOfTask TaskType
}

type DoneTaskResp struct {
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
