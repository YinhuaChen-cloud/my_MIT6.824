package mr

import "6.824/debug"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// 定义一个结构体类型：两个字符串变量，一个名为 Key，一个名为 Value 
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	askForTask()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 向 coordinator 请求任务
// 返回 true 表示请求到了任务
// 返回 false 表示没有请求到任务
func askForTask() bool {
    // machineID = IP地址 + PID
	machineID := "wudi"
    // 用来储存 coordinator 返回的任务
	reply := Task{}
	// send the RPC request, wait for the reply.
	retval := call("Coordinator.AssignTask", machineID, &reply)
    debug.Assert(retval, "do not consider rpc failure yet")



    // TODO: here 需要把 Machine 的 IP地址 + PID 作为 machineID
    return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// checked
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// checked
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
