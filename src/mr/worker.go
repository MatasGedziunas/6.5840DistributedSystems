package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	request := Request{}
	response := Response{}
	for response.Done == false {
		time.Sleep(1 * time.Second)
		ok := GetTask(&request, &response)
		if !ok {
			panic(fmt.Sprintf("Failure calling rpc, request: %v ; response %v", request, response))
		}
		// log.Printf("Got response %v ; Task: %v", response, response.Task)
		if response.Wait == true {
			// log.Printf("Waiting for next task")
			time.Sleep(1 / 2 * time.Second)
			response.Wait = false
			continue
		}
		if response.Done == true {
			ok = call("Coordinator.TaskDone", &Request{}, &Response{})
			if !ok {
				panic("Something wrong with calling rpc")
			}
			break
		}
		if response.Task.Type == MAP_TASK {
			Map(response.Filename, mapf, response.NReduce, response.NMap)
		} else if response.Task.Type == REDUCE_TASK {
			reduceTaskNumber, err := strconv.Atoi(response.Filename)
			check(err)
			Reduce(reduceTaskNumber, reducef)
		} else {
			fmt.Printf("No recognizable task type given")
		}
		request := Request{TaskDone: response.Task}
		ok = call("Coordinator.TaskDone", &request, &Response{})
		if !ok {
			panic("Something wrong with calling rpc")
		}
	}
}

func GetTask(request *Request, response *Response) bool {
	ok := call("Coordinator.GetNextTask", &request, &response)
	return ok
}

func Reduce(reduceTaskNumber int, reducef func(string, []string) string) error {
	files := getIntermediateFiles(reduceTaskNumber)
	// log.Printf("Reading reduceTaskNumber: %v ; len(files): %v", reduceTaskNumber, len(files))
	var allPairs []KeyValue
	for _, file := range files {
		reader, err := os.Open(file)
		check(err)
		dec := json.NewDecoder(reader)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if kv.Key == "abhor" {
				// log.Printf("In reduce: %v", kv)
			}
			allPairs = append(allPairs, kv)
		}
	}
	sort.Sort(ByKey(allPairs))
	oname := "mr-out-" + strconv.Itoa(reduceTaskNumber)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(allPairs) {
		j := i + 1
		for j < len(allPairs) && allPairs[j].Key == allPairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allPairs[k].Value)
		}
		output := reducef(allPairs[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allPairs[i].Key, output)

		i = j
	}
	return nil
}

func getIntermediateFiles(reduceTaskNumber int) []string {
	files := []string{}

	// Convert reduceTaskNumber to string for comparison
	suffix := "-" + strconv.Itoa(reduceTaskNumber)

	// Read all files in the current directory
	dirEntries, err := os.ReadDir(".")
	if err != nil {
		panic(err)
	}

	// Loop through the files in the directory
	for _, entry := range dirEntries {
		// Only consider files, skip directories
		if !entry.IsDir() {
			// Get the file name
			fileName := entry.Name()

			// Check if the file name ends with the correct reduce task number suffix
			if strings.HasSuffix(fileName, suffix) {
				files = append(files, fileName)
			}
		}
	}

	return files
}

func Map(filename string, mapf func(string, string) []KeyValue, nReduce int, nMap int) error {
	pathName, err := os.Getwd()
	pathName = pathName + "/" + filename
	check(err)
	contents, err := os.ReadFile(pathName)
	check(err)
	kvPairs := mapf(filename, string(contents))
	sort.Sort(ByKey(kvPairs))
	i := 0
	buckets := make([][]KeyValue, nReduce)
	for i < len(kvPairs) {
		if kvPairs[i].Key == "abhor" {
			// log.Printf("%v", kvPairs[i])
		}
		j := i + 1
		for j < len(kvPairs) && kvPairs[j].Key == kvPairs[i].Key {
			j++
		}
		bucket := ihash(kvPairs[i].Key) % nReduce
		for k := i; k < j; k++ {
			buckets[bucket] = append(buckets[bucket], kvPairs[k])
		}
		i = j
	}
	writeToIntermediateFiles(buckets, nMap)
	return nil
}

func writeToIntermediateFiles(buckets [][]KeyValue, nMap int) error {
	for index, bucket := range buckets {
		intermediateFileName := "mr-" + strconv.Itoa(nMap) + "-" + strconv.Itoa(index)
		dir, _ := os.Getwd()
		tmpFile, err := os.CreateTemp(dir, intermediateFileName)
		check(err)
		enc := json.NewEncoder(tmpFile)
		for _, kvPair := range bucket {
			if kvPair.Key == "abhor" {
				// log.Printf("writing to int files: %v", kvPair)
			}
			err := enc.Encode(kvPair)
			check(err)
		}
		fullPath := filepath.Join(dir, intermediateFileName)
		err = tmpFile.Close()
		check(err)
		err = os.Rename(tmpFile.Name(), fullPath)
		check(err)
	}
	return nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("Exiting worker:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
