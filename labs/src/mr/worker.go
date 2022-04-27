package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 向协调者获取一个任务
func callGetTask() *GetTaskResp {
	req := GetTaskReq{}
	resp := GetTaskResp{}
	call("Coordinator.GetTask", &req, &resp)
	return &resp
}

// 告诉协调者完成了任务
func callDoneTask(taskId int, taskType TaskType) {
	req := DoneTaskReq{
		TaskId:   taskId,
		TypeOfTask: taskType,
	}
	resp := DoneTaskResp{}
	call("Coordinator.DoneTask", &req, &resp)
}

//
// Worker main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 不断地去获取任务
	for {
		resp := callGetTask()
		switch resp.TypeOfTask {
		case MapType:
			handleMapTask(resp.Task, mapf)
		case ReduceType:
			handleReduceTask(resp.Task, reducef)
		case WaitType:
			time.Sleep(time.Microsecond*500)
		case ExitType:
			break
		}
	}

}

// 处理一个Map任务
func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.MapTask.Filename
	nReduce := task.MapTask.ReduceCount
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(filename + "无法正确打开")  // 打印提示信息并退出
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	buffer := make(map[int][]KeyValue)  // key为Reduce的id，value为这个map任务提取出来的属于该Reduce的KV对
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		buffer[index] = append(buffer[index], kv)
	}

	// 将buffer中的内容持久化到磁盘上
	for index, kvs := range buffer {
		// 格式化字符串，中间文件名为mr-X-Y，X为是由哪个Map任务生成，Y为将要被哪个Reduce任务处理
		outFileName := fmt.Sprintf("mr-%d-%d", task.TaskId, index)
		tempFile, err := ioutil.TempFile("", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot create %v", outFileName)
		}
		encoder := json.NewEncoder(tempFile)
		err = encoder.Encode(kvs)
		if err != nil {
			log.Fatalf("cannot write %v", outFileName)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), outFileName)
	}

	// 通知协调者完成了任务
	callDoneTask(task.TaskId, MapType)
	fmt.Println("worker完成了一个Map任务：" + string(rune(task.TaskId)))
}

// 处理一个Reduce任务
func handleReduceTask(task Task, reducef func(string, []string) string) {
	nMap := task.ReduceTask.MapCount
	var intermediate []KeyValue
	// 收集Map阶段产生的需要该Reduce去处理的键值对
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			//log.Fatalf(fileName + "无法打开")
			continue
		}
		var kvs []KeyValue
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&kvs); err != nil {
			log.Fatalf("cannot decode %v", fileName)
		}
		file.Close()
		intermediate = append(intermediate, kvs...)
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	tempFile, err := ioutil.TempFile("", "mr-reduce-*")
	if err != nil {
		log.Fatalf("cannot create %v", tempFile)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), oname)

	// 告诉协调者完成了该任务
	callDoneTask(task.TaskId, ReduceType)
	fmt.Println("worker完成了一个Reduce任务：" + string(rune(task.TaskId)))
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
