package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	files   []string
	mapQ    Queue // Maybe could just be one queue for all tasks :P
	reduceQ Queue
	lock    sync.Mutex
	nReduce int
}

func (c *Coordinator) GetNextTask(request *Request, response *Response) error {
	if c.mapQ.AllTasksFinished() { // map files done
		return c.GetNextReduceTask(request, response)
	} else {
		return c.GetNextMapTask(request, response)
	}
}

func (c *Coordinator) GetNextMapTask(request *Request, response *Response) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	task, isEmpty := c.mapQ.GetFirstIdleTask()
	if isEmpty == true {
		// log.Printf("WAIT FOR OTHER TASKS TO FINISH:\n")
		// c.mapQ.Print()
		response.Wait = true
	} else {
		fileNumber, _ := strconv.Atoi(task.MapTaskNumber)
		task.StartTask()
		// log.Printf("Sending MAP task: %v", *task)
		response.SetResponse(c.files[fileNumber], false, *task, c.nReduce, fileNumber)
	}
	return nil
}

func (c *Coordinator) GetNextReduceTask(request *Request, response *Response) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	task, isEmpty := c.reduceQ.GetFirstIdleTask()
	if isEmpty == true {
		response.Wait = true
	} else {
		taskNumberInt, _ := strconv.Atoi(task.ReduceTaskNumber)
		// log.Printf("Sending REDUCE task: %v", *task)
		response.SetResponse(task.ReduceTaskNumber, c.reduceQ.Len() == 0, *task, c.nReduce, taskNumberInt)
	}
	return nil
}

func (c *Coordinator) TaskDone(request *Request, response *Response) error {
	// register which task has been done, so need task in request.
	var q Queue
	if request.TaskDone.Type == REDUCE_TASK {
		q = c.reduceQ
	} else if request.TaskDone.Type == MAP_TASK {
		q = c.mapQ
	} else {
		panic(fmt.Sprintf("TaskDone: request's task type is invalid: %s", request.TaskDone.Type))
	}
	found := q.FinishTask(request.TaskDone)
	if found == false {
		panic("trying to finish a task which does not exist?")
	}
	// log.Printf("Finished task: %v", request.TaskDone)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// log.Printf("counter: %v, reduceIndex: %v ; nReduce: %v", c.counter, c.reduceIndex, c.nReduce)
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := c.reduceQ.AllTasksFinished()
	if ret == true {
		log.Print("Job is finished")
	}
	return ret
}

func (c *Coordinator) CheckWorkers() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.lock.Lock()
			for _, task := range c.mapQ.tasks {
				if task.State == PROGRESS_STATE && time.Since(task.TimeStarted) > 10*time.Second {
					// log.Printf("Reseting task: %v", task)
					task.SetState(IDLE_STATE)
				}
			}
			for _, task := range c.reduceQ.tasks {
				if task.State == PROGRESS_STATE && time.Since(task.TimeStarted) > 10*time.Second {
					// log.Printf("Reseting task: %v", task)
					task.SetState(IDLE_STATE)
				}
			}
			c.lock.Unlock()
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files,
		nReduce: nReduce,
		reduceQ: NewQueue(nReduce, REDUCE_TASK),
		mapQ:    NewQueue(len(files), MAP_TASK)} // would be better to split files
	c.server()
	go c.CheckWorkers()
	return &c
}
