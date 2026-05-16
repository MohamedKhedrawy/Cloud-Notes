package mr

import "time"

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//enumuration
type TaskType int

const (
	//iota is just a counter generator in go, starting from 0.
	MAP TaskType = iota
	REDUCE
	WAIT
	EXIT
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	INPROGRESS
	DONE
)

type TaskState struct {
	TaskStatus TaskStatus
	CurTime time.Time
}

type TaskArgs struct {
}


type Task struct {
	FileName string
	TaskType TaskType
	TaskState TaskState
	TaskID int
	NReduce int

}

type TaskDoneArgs struct {
	TaskID int
	TaskType TaskType
}

type TaskDoneReply struct {
	Ok bool
}


type KeyValue struct {
	Key   string
	Value string
}

type Bucket struct {
	KV []KeyValue
}
