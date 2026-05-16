package mr

import (
	// "fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)




type Coordinator struct {
	// Your definitions here.
	Tasks []Task
	TasksCount int
	Phase TaskType
	isDone bool
	mu sync.Mutex
	NReduce int
}

func (c *Coordinator) Task(args *TaskArgs, reply *Task) error {
	if c.isDone {
		reply.TaskType = EXIT
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.TaskType = WAIT
	if c.Phase == MAP {
		for i, task := range c.Tasks {
			if task.TaskState.TaskStatus != IDLE {
				continue
			}
			reply.FileName = task.FileName
			reply.TaskType = task.TaskType
			reply.TaskID = task.TaskID
			reply.NReduce = c.NReduce
			c.Tasks[i].TaskState.TaskStatus = INPROGRESS
			c.Tasks[i].TaskState.CurTime = time.Now()

			go func (taskID int, phase TaskType)  {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				
				if c.Phase == phase && c.Tasks[taskID].TaskState.TaskStatus == INPROGRESS {
					c.Tasks[taskID].TaskState.TaskStatus = IDLE
				}
			} (task.TaskID, c.Phase)

			break
		}
	} else if c.Phase == REDUCE {
		for i, task := range c.Tasks {
			if task.TaskState.TaskStatus != IDLE {
				continue
			}
			reply.TaskType = task.TaskType
			reply.TaskID = task.TaskID
			reply.NReduce = c.NReduce
			c.Tasks[i].TaskState.TaskStatus = INPROGRESS
			c.Tasks[i].TaskState.CurTime = time.Now()

			go func (taskID int, phase TaskType)  {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				
				if c.Phase == phase && c.Tasks[taskID].TaskState.TaskStatus == INPROGRESS {
					c.Tasks[taskID].TaskState.TaskStatus = IDLE
				}
			} (task.TaskID, c.Phase)

			break
		}

	}
	return nil
}


func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.Phase != args.TaskType {
		return nil 
	}
	
	if args.TaskType == MAP {
		if c.Tasks[args.TaskID].TaskState.TaskStatus == DONE {
			return fmt.Errorf("Error: Task already completed")
		}
		c.Tasks[args.TaskID].TaskState.TaskStatus = DONE
		reply.Ok = true
		c.TasksCount--
		if c.TasksCount <= 0 {
			c.Phase = REDUCE
			c.Tasks = []Task{}
			for i := range c.NReduce {
				c.Tasks = append(c.Tasks, Task{
					TaskID: i,
					TaskType: REDUCE,
					TaskState: TaskState{TaskStatus:  IDLE},
				})
				c.TasksCount = c.NReduce
			}
		}
	} else if args.TaskType == REDUCE {
		if c.Tasks[args.TaskID].TaskState.TaskStatus == DONE {
			return fmt.Errorf("Error: Task already completed")
		}
		c.Tasks[args.TaskID].TaskState.TaskStatus = DONE
		reply.Ok = true
		c.TasksCount--
		if c.TasksCount <= 0 {
			c.isDone = true
		}
	}
	return nil
} 

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.isDone

	// Your code here.


	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Phase = MAP
	c.TasksCount = len(files)
	c.NReduce = nReduce
	for i := range c.TasksCount {
		c.Tasks = append(c.Tasks, Task{
			FileName: files[i],
			TaskType: MAP,
			TaskID: i,
			TaskState: TaskState{TaskStatus: IDLE},
			NReduce: c.NReduce,
		})
	}

	c.server(sockname)
	return &c
}
