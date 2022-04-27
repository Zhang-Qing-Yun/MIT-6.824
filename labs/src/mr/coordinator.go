package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int

type TaskStatus int

type RespTaskType int

var (
	MapPhase Phase = 1
	ReducePhase Phase = 2
)

var (
	Idle TaskStatus = 1
	Doing TaskStatus = 2
	Done TaskStatus = 3
)

type Task struct {
	TaskId int
	TaskStatus TaskStatus  // 该任务的状态

	MapTask    MapTask    // 处于Map阶段使用该属性
	ReduceTask ReduceTask // 处于Reduce阶段使用该属性
}

type MapTask struct {
	Filename    string // 该Map任务所要处理的文件的文件名
	ReduceCount int
}

type ReduceTask struct {
	MapCount int
}

type Coordinator struct {
	phase Phase  // 当前是哪个阶段
	nMap int  // 一共多少个Map任务
	nReduce int  // 一共多少个Reduce
	done bool  // 是否完成了全部Map和Reduce任务
	tasks []Task  // 任务列表，阶段不同任务类型不同
	taskTimeout map[int]time.Time  // key为taskId，value为task开始执行的时间

	taskChan chan GetTaskMsg
	doneChan chan DoneTaskMsg
}

// GetTaskMsg 封装向协调者获取任务的消息
type GetTaskMsg struct {
	resp *GetTaskResp
	waitChan chan struct{}  // 用channel来完成等待通知功能，协调者返回消息后worker才能解除阻塞
}

// DoneTaskMsg 封装了worker向协调者发送的完成任务的消息
type DoneTaskMsg struct {
	req *DoneTaskReq
	waitChan chan struct{}
}

// schedule 作用是用不同的方式来处理coordinator收到的不同消息
func (c *Coordinator) schedule() {
	for {
		select {
		case msg := <- c.taskChan:
			c.handleGetTaskMsg(msg)
		case msg := <- c.doneChan:
			c.handleDoneTaskMsg(msg)
		//case <- time.After(time.Second * 10):  // 每次执行select都会重新开始计时
		//	c.handleTimeout()
		}

	}
}

// 处理获取任务的消息
func (c *Coordinator) handleGetTaskMsg(msg GetTaskMsg) {
	//resp := msg.resp
	allDone := true
	// 寻找空闲任务
	for i, task := range c.tasks {
		if task.TaskStatus != Done {
			allDone = false
		}
		if task.TaskStatus == Idle {
			if c.phase == MapPhase {
				msg.resp.TypeOfTask = MapType
			} else {
				msg.resp.TypeOfTask = ReduceType
			}
			msg.resp.Task = task

			c.tasks[i].TaskStatus = Doing
			c.taskTimeout[task.TaskId] = time.Now()
			// 可以解除阻塞了
			msg.waitChan <- struct{}{}
			return
		}
	}
	// 没有空闲任务
	if c.phase == MapPhase {
		// 此处无论是否完成了Map阶段的所有任务都直接Wait，切换到Reduce阶段是在worker向协调者发送done消息时进行
		msg.resp.TypeOfTask = WaitType
		msg.waitChan <- struct{}{}
	} else {
		if allDone {
			// 当前是Reduce阶段并且所有的任务都完成了，可以退出了
			msg.resp.TypeOfTask = ExitType
			msg.waitChan <- struct{}{}
		} else {
			msg.resp.TypeOfTask = WaitType
			msg.waitChan <- struct{}{}
		}
	}
}

// 处理worker提交的完成任务的消息
func (c *Coordinator) handleDoneTaskMsg(msg DoneTaskMsg) {
	req := msg.req
	// 处理重复提交
	if c.phase == ReducePhase && req.TypeOfTask == MapType {
		msg.waitChan <- struct{}{}
		return
	}
	for i, task := range c.tasks {
		if task.TaskId == req.TaskId {
			c.tasks[i].TaskStatus = Done
			break
		}
	}
	delete(c.taskTimeout, req.TaskId)

	// 判断是否处理完了本阶段的所有任务
	allDone := true
	for _, task := range c.tasks {
		if task.TaskStatus != Done {
			allDone = false
			break
		}
	}
	if allDone {
		if c.phase == MapPhase {
			// 进入并初始化Reduce阶段
			c.initReducePhase()
			fmt.Printf("进入了Reduce阶段")
		} else {
			c.done = true
		}
	}
	msg.waitChan <- struct{}{}
}

//  进行超时检查，取消掉超过10s还没有完成的任务
func (c *Coordinator) handleTimeout() {
	now := time.Now()
	for key, value := range c.taskTimeout {
		if now.Sub(value).Seconds() > 10 {
			for i, task := range c.tasks {
				if task.TaskId == key {
					if task.TaskStatus != Done {
						c.tasks[i].TaskStatus = Idle
						break
					}
				}
			}
			delete(c.taskTimeout, key)
		}
	}
}

// 初始化Map阶段
func (c *Coordinator) initMapPhase(fileNames []string) {
	c.phase = MapPhase
	c.taskTimeout = make(map[int]time.Time)
	c.tasks = []Task{}
	for i, fileName := range fileNames {
		c.tasks = append(c.tasks, Task{
			TaskId: i,
			TaskStatus: Idle,
			MapTask: MapTask{fileName, c.nReduce},
		})
	}
}

// 初始化Reduce阶段
func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.taskTimeout = make(map[int]time.Time)
	c.tasks = []Task{}
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, Task{
			TaskId: i,
			TaskStatus: Idle,
			ReduceTask: ReduceTask{
				c.nMap,
			},
		})
	}
}

// GetTask 用于rpc调用的函数，获取一个任务
func (c *Coordinator) GetTask(_ *GetTaskReq, resp *GetTaskResp) error {
	msg := GetTaskMsg{
		resp: resp,
		waitChan: make(chan struct{}),
	}
	c.taskChan <- msg
	<- msg.waitChan
	return nil
}

// DoneTask 用于rpc调用的函数，worker通知协调者完成了一个任务
func (c *Coordinator) DoneTask(req *DoneTaskReq, _ *DoneTaskResp) error {
	msg := DoneTaskMsg{
		req: req,
		waitChan: make(chan struct{}),
	}
	c.doneChan <- msg
	<- msg.waitChan
	return nil
}

//
// server start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.done
}

//
// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap: len(files),
		nReduce: nReduce,
		done: false,
		taskChan: make(chan GetTaskMsg),
		doneChan: make(chan DoneTaskMsg),
	}
	c.initMapPhase(files)
	go c.schedule()
	go func() {
		ticker := time.NewTicker(time.Second * 10)  // 触发器，每隔一段时间触发一次，即向channel中写入当前时间
		for {
			<- ticker.C
			c.handleTimeout()
		}
	}()

	c.server()
	return &c
}
