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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	// 任务状态
	Status int
	// Machine ID : IP地址 + PID
	MachineID string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 返回一个带有用户 ID 的字符串，作为套接字名称
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	// os.Getuid() 用于获取当前进程的用户 ID（UID）
	s += strconv.Itoa(os.Getuid())
	return s
}
