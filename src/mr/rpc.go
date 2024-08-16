package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"strconv"
	"time"
)

const MAP_TASK = "map"
const REDUCE_TASK = "reduce"
const IDLE_STATE = "idle"
const PROGRESS_STATE = "progress"
const COMPLETED_STATE = "completed"

type Request struct {
	TaskDone Task
}

type Response struct {
	Filename string
	Done     bool
	Task     Task
	NReduce  int
	NMap     int
	Wait     bool
}

func (r *Response) SetResponse(Filename string, Done bool, Task Task, NReduce int, NMap int) {
	r.Filename = Filename
	r.Done = Done
	r.Task = Task
	r.NReduce = NReduce
	r.NMap = NMap
}

type Task struct {
	Type             string
	ReduceTaskNumber string
	MapTaskNumber    string
	State            string
	TimeStarted      time.Time
}

func NewTask(Type string, MapTaskNumber string, ReduceTaskNumber string) Task {
	return Task{Type: Type, ReduceTaskNumber: ReduceTaskNumber, MapTaskNumber: MapTaskNumber, State: IDLE_STATE}
} // could be done better :P

func (t *Task) SetState(state string) {
	t.State = state
}

func (t *Task) StartTask() {
	t.State = PROGRESS_STATE
	t.TimeStarted = time.Now()
}

func (t *Task) EndTask() {
	t.State = COMPLETED_STATE
}

type Queue struct {
	tasks []*Task // Maybe should be 3 different queues which would contain tasks for each different state
	// should also probably be a hashmap :P
}

func NewQueue(taskCount int, taskType string) Queue {
	var tasks []*Task
	for i := 0; i < taskCount; i++ {
		var temp Task
		if taskType == MAP_TASK { // could be done better :P
			temp = NewTask(taskType, strconv.Itoa(i), "")
		} else {
			temp = NewTask(taskType, "", strconv.Itoa(i))
		}
		tasks = append(tasks, &temp)
	}
	return Queue{tasks: tasks}
}

func (q *Queue) Print() {
	for i, v := range q.tasks {
		log.Printf("task %v: %v", i, v)
	}
}

func (q *Queue) Enqueue(val *Task) {
	q.tasks = append(q.tasks, val)
}

func (q *Queue) Dequeue() (*Task, bool) {
	if len(q.tasks) == 0 {
		return &Task{}, true // Queue is empty
	}
	val := q.tasks[0]
	q.tasks = q.tasks[1:]
	return val, false
}

func (q *Queue) Front() (*Task, bool) {
	if len(q.tasks) == 0 {
		return &Task{}, true // Queue is empty
	}
	return q.tasks[0], false
}

func (q *Queue) GetFirstIdleTask() (*Task, bool) {
	if len(q.tasks) == 0 {
		return &Task{}, true // Queue is empty
	}
	// log.Printf("%v", q.tasks)
	for _, task := range q.tasks {
		if task.State == IDLE_STATE {
			return task, false
		}
	}
	return &Task{}, true
}

func (q *Queue) AllTasksFinished() bool {
	for _, task := range q.tasks {
		if task.State != COMPLETED_STATE {
			return false
		}
	}
	return true
}

func (q *Queue) FinishTask(t Task) bool {
	for _, task := range q.tasks {
		if task.Type == t.Type && task.Type == REDUCE_TASK && task.ReduceTaskNumber == t.ReduceTaskNumber {
			task.EndTask()
			return true
		} else if task.Type == t.Type && task.Type == MAP_TASK && task.MapTaskNumber == t.MapTaskNumber {
			task.EndTask()
			return true
		}
	}
	return false
}

func (q *Queue) Len() int {
	return len(q.tasks)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
