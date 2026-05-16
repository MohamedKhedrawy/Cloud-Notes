package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.

// for sorting by key.
type ByKey []KeyValue

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


var coordSockName string // socket for coordinator

func encode(file io.Writer, kv KeyValue) {
	enc := json.NewEncoder(file)
	enc.Encode(&kv)
}

func decode (file io.Reader) []KeyValue {
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// Your worker implementation here.
	for {
		ok, reply := askTask()

		if !ok || reply.TaskType == EXIT {
			break
		}
		
		if ok {
			if reply.TaskType == MAP {
				file, err := os.Open(reply.FileName)
				if err != nil {
					fmt.Printf("Error: %s", err)
				}
				contents, err := io.ReadAll(file)
				file.Close()
				var bucketNo int

				if err != nil {
					fmt.Printf("Error: %s", err)
				}

				kva := mapf(reply.FileName, string(contents))
				var files = []*os.File{}
				for _ = range reply.NReduce {
					ofile, err := os.CreateTemp("./", "mr-map-tmp")
					if err != nil {
						fmt.Printf("Error: %s", err)
					}
					files = append(files, ofile)
				}
				for _, kv := range kva {
					bucketNo = ihash(kv.Key) % reply.NReduce
					if err != nil {
						fmt.Printf("Error: %s", err)
					}
					encode(files[bucketNo], kv)
				}

				for i, file := range files {
					fileOut := fmt.Sprintf("mr-%s-%d", strconv.Itoa(reply.TaskID), i)
					file.Close()
					os.Rename(file.Name(), fileOut)
				}

				args := TaskDoneArgs {
					TaskType: MAP,
					TaskID: reply.TaskID,
				}

				reply := TaskDoneReply{}

				call("Coordinator.TaskDone", &args, &reply)

			} else if reply.TaskType == REDUCE {
				fileIn := fmt.Sprintf("mr-*-%s", strconv.Itoa(reply.TaskID))
				files, err := filepath.Glob(fileIn)
				var kva []KeyValue
				
				if err != nil {
					fmt.Printf("Error: %s", err)
				}
				for _, file := range(files) {
					reader, err := os.Open(file)
					if err != nil {
						fmt.Printf("Error: %s", err)

					}
					inter := decode(reader)
					kva = append(kva, inter...)
				}
				sort.Sort(ByKey(kva))
				var prev KeyValue
				values := []string{}
				ofile, err := os.CreateTemp("./", "mr-out-tmp")
				if err != nil {
					fmt.Printf("Error: %v", err)
				}
				if err != nil {
					fmt.Printf("Error: %s", err)
				}
				for i, kv := range kva {
					if prev.Key != kv.Key && i > 0 { //[kv1, kv1, kv2, kv2, kv3]
						reduceResult := reducef(prev.Key, values)
						values = []string{}
						fmt.Fprintf(ofile, "%v %v\n", prev.Key, reduceResult)
					}
					prev = kv
					values = append(values, kv.Value)
						if i >= len(kva) - 1 {
							reduceResult := reducef(kv.Key, values)
							fmt.Fprintf(ofile, "%v %v\n", kv.Key, reduceResult)
						}
				}
				fileRename := fmt.Sprintf("mr-out-%s", strconv.Itoa(reply.TaskID))
				ofile.Close()
				os.Rename(ofile.Name(), fileRename)
				args := TaskDoneArgs {
					TaskType: REDUCE,
					TaskID: reply.TaskID,
				}

				reply := TaskDoneReply{}

				call("Coordinator.TaskDone", &args, &reply)

			} else if reply.TaskType == WAIT {
				time.Sleep(time.Second)
			}
		}
	}
}

func askTask() (bool, Task) {
	args := TaskArgs{}
	reply := Task{}

	ok := call("Coordinator.Task", &args, &reply)
	if ok {
		return true, reply
	} else {
		fmt.Printf("call failed!\n")
		return false, reply
	}
}


func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
