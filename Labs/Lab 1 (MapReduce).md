The Goal: Implement a distributed (simulated) MapReduce job 

Map Phase: Each worker reads one input file and for every word it generates a kv pair of (word: 1) meaning this word is present.

Reduce Phase: Each worker collects all the kv pairs with the same key and then counts the length of the array of the values of the key. The values don't matter as they're all 1 but how many 1s represents how many words there are.

The files to edit:

	`mr/coordinator.go`        The coordinator — assigns map & reduce tasks

	`mr/worker.go`             The worker — asks for tasks, runs them

	`mr/rpc.go`                The message format between them (structs)

Important Files:
	`main/mrsequential.go       Working single-process version

	`mrapps/wc.go`             The map/reduce functions you'll call

	`main/mrcoordinator.go`    Entry point for coordinator

	`main/mrworker.go`         Entry point for worker

Thought Process:
- We first need to run the mrsequential.go and see the output.
- We should read the main entry points main/mrcoordinator and main/mrworker and see the structure and the functions they call .
- Then we read mr/coordinator and mr/worker and mr/rpc to figure out the connection to the entry points and the missing parts we need to implement.
- I think we should start by defining the arguments of the AssignTask (that replaces ExampleTask) function in the rpc.go file. (PS. every function should follow the rpc signature of 2 arguments and return error in order to be callable by the workers )
- We'd ultimately want to pass an input file to either the Map or Reduce functions. (Map takes a raw input file while Reduce takes a json file of the intermediate results, anyway we just pass a file path)
- Along with the input file we should define the type of the task (Map - Reduce), and an ID to track it.
- The reply should be an array of KV pairs for both (maybe in a file format)
```go
type TaskArgs struct {

	FileName string
	
	TaskType TaskType
	
	TaskID int
	
	NumTasks int

}

type TaskReply struct {

	FileName string
	
	TaskType TaskType
	
	TaskID int

}

//enumuration

type TaskType int

const (
	//iota is just a counter generator in go, starting from 0.
	MAP TaskType = iota
	
	REDUCE

)
```

- I misunderstood the args, reply structure. It is actually the other way around. the workers are the ones to call the coordinator and ask for a task (with the args as the parameters, maybe a workerID) and the coordinator replies with a task along with the needed arguments as the reply.
- When the worker finishes it calls the coordinator again notifying it that it has finished, and the coordinator just acknowledges.
- After looking at the function signatures in wc.go file. We also need to include a contents string along with the filename string. 
- New suggested structs:
```go
type TaskArgs struct {

	//idk if i should specify the type of task, we'll see
	
	TaskType TaskType
	WorkerID int

}

type TaskReply struct {

	FileName string
	TaskType TaskType
	TaskID int

}
```

- Now let's explore how to use the rpc.
- We should write an askTask function in the worker that calls the Task rpc call and asks the coordinator for tasks, i decided for it to return the process id if it successfully got a task from the coordinator. (I'll write its rpc call when i understand more)
```go
func askTask() (bool, TaskReply) {

	args := TaskArgs{
		WorkerID: os.Getpid(),
	}
	
	reply := TaskReply{}
	ok := call("Coordinator.Task", &args, &reply)
	
	if ok {
		return true, reply
	} else {
		fmt.Printf("call failed!\n")
		return false, reply
	
	}

}
```

- I also wrote another rpc call called TaskDone that the worker calls to pass the coordinator the results and notify it that it's done.
```go
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {

	if args.TaskType == MAP {
		kv := args.InterKV
		intermediate = append(intermediate, kv...)
		
	} else if args.TaskType == REDUCE {
		reduceResult := args.ReduceResult
		file, err := os.OpenFile("mr-out-0",os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	
		if err != nil {
			fmt.Printf("Error: %e \n", err)
			return err	
		}
	
		defer file.Close()	
		fmt.Fprintf(file, reduceResult)
	}
	
	return nil
}
```

- I connected these 2 rpc calls to the worker function:
```go
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// Your worker implementation here.
	ok, reply := askTask()

	if ok {
		if reply.TaskType == MAP {
			file, err := os.Open(reply.FileName)
			if err != nil {
				contents, err := io.ReadAll(file)
				if err != nil {
					kv := mapf(reply.FileName, string(contents))
					args := TaskDoneArgs {
						WorkerID: os.Getpid(),
						TaskType: MAP,
						InterKV: kv,
					}

					reply := TaskDoneReply{}

					call("Coordinator.TaskDone", &args, &reply)
				}
			}
		} else {
			reduceResult := reducef(reply.Key, reply.Values)
			args := TaskDoneArgs {
				WorkerID: os.Getpid(),
				TaskType: REDUCE,
				ReduceResult: reduceResult,
			}

			reply := TaskDoneReply{}

			call("Coordinator.TaskDone", &args, &reply)
		}
	}
}
```

- We missed a critical step. We didn't use the hash function to split the intermediate output into buckets. The hash function ensures that the same key always goes to the same bucket.
- I've created a new struct type (Bucket) and added a snippet in the worker function as follows, I've also added a ReduceBuckets field to the TaskDone rpc struct and now I pass it in the args.
```go
type Bucket struct {
	KV []KeyValue
}

buckets := make([]Bucket, 10)
kva := mapf(reply.FileName, string(contents))
for _, kv := range kva {
	bucketNo := ihash(kv.Key) % 10
	buckets[bucketNo].KV = append(buckets[bucketNo].KV, kv) 
}
```

- Now, the worker supposedly can call the coordinator for a task and report when it's done. I guess the next step is to code the Task rpc call and the MakeCoordinator function to assign task and track progress.
- We need to use buckets everywhere else.
- I wrote some Coordinator struct definitions:
```go
type Coordinator struct {
	// Your definitions here.
	Tasks []Task
	TasksCount int
	TaskPointer int
	Phase TaskType
	WorkersIds []int
	isDone bool
	Buckets []Bucket

}
```

- And the task rpc:
```go
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.WorkersIds = append(c.WorkersIds, args.WorkerID)
	if c.Phase == MAP {
		task := c.Tasks[c.TaskPointer]
		reply.FileName = task.FileName
		reply.TaskType = task.TaskType
		reply.TaskID = task.TaskID
	} else if c.Phase == REDUCE {
		task := c.Tasks[c.TaskPointer]
		reply.Bucket = task.Bucket
		reply.TaskType = task.TaskType
		reply.TaskID = task.TaskID

	}
	return nil
}
```
