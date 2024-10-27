package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// Your code here -- RPC handlers for the worker to call.
// 定义枚举常量: workers状态
const (
    Idle = iota     // 0
    InProgress      // 1
    Completed       // 2
)

type Coordinator struct {
	// Your definitions here.
    // 任务的状态 (停止、运行、完成)
    MapTasks []Task
    ReduceTasks []Task
    // 剩余的 Map 和 Reduce 任务数量
    RemainMap int
    RemainReduce int
}

// 在 Go 语言中，结构体方法名是否大写决定了它的可见性，但并非必须大写。这关系到 Go 的访问控制规则：
// 大写方法：对外部包可见（导出）。如果你想让结构体的方法在其他包中也可以被调用，那么方法名需要大写。
// 小写方法：仅对当前包可见（未导出）。如果方法名是小写的，那么它只能在定义它的包内部使用，其他包无法访问。
// 优先分配 Map 任务，如果没有 Map 任务，则根据是否有 Intermediate 来分配 Reduce 任务
func (c *Coordinator) AssignTask(machineID string, reply *Task) error {
    fmt.Printf("hahaha %s\n", machineID)
    // 优先分配 Map 任务
    if c.RemainMap > 0 {
        // // 若还有剩余的 Map 任务
        // for i=0; i < len(c.MapTasks); i++ {
        //     if c.MapTasks[i].Status == Idle {
        //         c.MapTasks[i].Status = InProgress
        //         // c.MapTasks[i].MachineID = InProgress // TODO: here
        //         c.RemainMap--
        //         // Task // TODO: 修改 Task 返回参数
        //         break
        //     }
        // }
    } else if c.RemainReduce > 0 {
        // 若还有剩余的 Reduce 任务
    } else {
        // 若已经没有任务
    }

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// checked
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
// checked
func (c *Coordinator) server() {
	// 注册 c（即 Coordinator 实例）为 RPC 服务。
	// 这个步骤会让 RPC 库识别 Coordinator 的方法，使其可通过 RPC 访问
	rpc.Register(c)
	// 将 RPC 服务绑定到 HTTP 处理程序，使得 RPC 请求可以通过 HTTP 协议来访问。
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// sockname = 824-mr-1000
	sockname := coordinatorSock()
	// os.Remove(sockname): 删除已有的同名套接字文件，避免绑定失败。
	os.Remove(sockname)
	// 使用 Unix 套接字协议创建监听器，监听之前生成的套接字名称 sockname。
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 启动一个新的 goroutine，在该 goroutine 中使用 http.Serve 处理 HTTP 请求，
	// 传入监听器 l 和默认处理器 nil。
	// 在 Go 语言中，goroutine 是一种轻量级的执行单元，它可以被视为协作式线程。
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// checked
func (c *Coordinator) Done() bool {
	done := true
	// Your code here.
    // 检查是否所有 Map 任务完成
    for i := 0; i < len(c.MapTasks); i++ {
        if c.MapTasks[i].Status != Completed {
            done = false
        }
    }
    // 检查是否所有 Reduce 任务完成
    for i := 0; i < len(c.ReduceTasks); i++ {
        if c.ReduceTasks[i].Status != Completed {
            done = false
        }
    }
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
    // Map 任务数量和文件数量一致，Reduce 任务数量由参数 nReduce 决定
    nMap := len(files)
    c.MapTasks = make([]Task, nMap)
	c.ReduceTasks = make([]Task, nReduce)
    c.RemainMap = nMap
    c.RemainReduce = nReduce

    // 所有任务状态设置为空闲
    for i := 0; i < nMap; i++ {
        c.MapTasks[i].Status = Idle
    }
    for i := 0; i < nReduce; i++ {
        c.ReduceTasks[i].Status = Idle
    }

	c.server()
	return &c
}
