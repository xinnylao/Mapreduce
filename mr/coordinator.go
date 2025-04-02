package mr

import (
	// "fmt"
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
	/*DEFINE ATTRIBUTES FOR MASTER HERE*/
	files              []string
	filesToMap         chan FilesPackage
	nReduce            int
	intermediateFiles  []string
	lock               *sync.RWMutex
	mapTaskOverview    map[int]string
	reduceTaskOverview map[int]string
	buckets            chan BucketPackage
}

type FilesPackage struct {
	file       string
	fileNumber int
}

type BucketPackage struct {
	bucket       []string
	bucketNumber int
}

// Your code here -- RPC handlers for the worker to call.
/*WORKER AND COORDINATOR COMMUNIATES VIA RPC*/

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *Empty, reply *ReplyTask) error {
	// fmt.Println("ENTERS REQUESTTASK")
	/*TO GET STARTED: WORKER REQUESTS A TASK FROM MASTER*/
	/*MASTER SHOULD RESPOND WITH EITHER A MAP OR REDUCE TASK*/
	// fmt.Println("LENGTH OF INTERMEDIATE FILES: ", len(c.intermediateFiles))
	// fmt.Println("LENGTH OF MXN: ", c.nReduce, "*", len(c.files))
	c.lock.RLock()
	lenIntFiles := len(c.intermediateFiles)
	targetlenIntFiles := c.nReduce * len(c.files)
	lenReduceTasks := len(c.reduceTaskOverview)
	copyNReduce := c.nReduce
	c.lock.RUnlock()
	if lenIntFiles != targetlenIntFiles {
		giveMapTask(c, reply)
	} else if lenReduceTasks != copyNReduce {
		go processIntermediateFiles(c)
	} else {
		giveReduceTask(c, reply)
	}
	// fmt.Println("EXITS REQUESTTASK")
	return nil
}

func giveMapTask(c *Coordinator, reply *ReplyTask) {
	// fmt.Println("ENTERS GIVEMAPTASK")
	c.lock.RLock()
	checkLen := len(c.filesToMap)
	c.lock.RUnlock()
	if checkLen == 0 {
		// fmt.Println("EXITS GIVEMAPTASK")
		return
	}
	reply.TaskType = "map"
	filePackage := <-c.filesToMap
	reply.MapTask = MapTask{
		FileName:      filePackage.file,
		MapTaskNumber: filePackage.fileNumber,
	}
	reply.ReduceTask = ReduceTask{}
	reply.NReduce = c.nReduce
	c.lock.Lock()
	c.mapTaskOverview[filePackage.fileNumber] = "map started"
	c.lock.Unlock()
	go followStatusMap(c, filePackage.fileNumber)
	// fmt.Println("EXITS GIVEMAPTASK")
}

func processIntermediateFiles(c *Coordinator) {
	// fmt.Println("ENTERS PROCESSINTERMEDIATEFILES")
	sortBuckets := make([][]string, c.nReduce)
	c.lock.Lock()
	for _, intFileName := range c.intermediateFiles {
		bucketIndex := int(intFileName[len(intFileName)-1] - '0')
		sortBuckets[bucketIndex] = append(sortBuckets[bucketIndex], intFileName)
		c.reduceTaskOverview[bucketIndex] = "reduce ready"
	}
	c.lock.Unlock()
	for index, bucket := range sortBuckets {
		c.buckets <- BucketPackage{bucket, index}
	}
	// fmt.Println("EXITS PROCESSINTERMEDIATEFILES")
}

func giveReduceTask(c *Coordinator, reply *ReplyTask) {
	// fmt.Println("ENTERS GIVEREDUCETASK")
	// c.lock.RLock()
	reply.TaskType = "reduce"
	reply.MapTask = MapTask{}
	bucketPackage := <-c.buckets
	reply.ReduceTask = ReduceTask{
		Bucket:           bucketPackage.bucket,
		ReduceTaskNumber: bucketPackage.bucketNumber,
	}
	reply.NReduce = c.nReduce
	// c.lock.RUnlock()
	c.lock.Lock()
	c.reduceTaskOverview[bucketPackage.bucketNumber] = "reduce started"
	c.lock.Unlock()
	go followStatusReduce(c, bucketPackage.bucketNumber)
	// fmt.Println("EXITS GIVEREDUCETASK")
}

func (c *Coordinator) ReturnMapResult(args *MapResult, reply *Empty) error {
	// fmt.Println("ENTERS RETURNMAPRESULT")
	c.lock.RLock()
	copyStatus := c.mapTaskOverview[args.MapTask.MapTaskNumber]
	c.lock.RUnlock()
	if copyStatus != "map finished" {
		c.lock.Lock()
		c.intermediateFiles = append(c.intermediateFiles, args.IntFiles...)
		// fmt.Println("INTERMEDIATE FILES: ", c.intermediateFiles)
		c.mapTaskOverview[args.MapTask.MapTaskNumber] = "map finished"
		c.lock.Unlock()
	}
	// fmt.Println("EXITS RETURNMAPRESULT")
	return nil
}

func (c *Coordinator) ReturnReduceResult(args int, reply *Empty) error {
	// fmt.Println("ENTERS RETURNREDUCERESULT")
	c.lock.Lock()
	defer c.lock.Unlock()
	c.reduceTaskOverview[args] = "reduce finished"
	// fmt.Println("EXITS RETURNREDUCERESULT")
	return nil
}

func followStatusMap(c *Coordinator, taskNumber int) {
	// fmt.Println("ENTERS FOLLOWSTATUSMAP WITH TASKNUMBER ", taskNumber)
	time.Sleep(10 * time.Second)
	c.lock.RLock()
	if _, ok := c.mapTaskOverview[taskNumber]; ok && c.mapTaskOverview[taskNumber] != "map finished" {
		// fmt.Println("TASK ", taskNumber)
		c.filesToMap <- FilesPackage{c.files[taskNumber], taskNumber}
		// fmt.Println("TASK ", taskNumber, " DID NOT RETURN IN TIME")
		// fmt.Println("AFTER ADDING TASK ", taskNumber)
	}
	c.lock.RUnlock()
	// fmt.Println("EXITS FOLLOWSTATUSMAP")
}

func followStatusReduce(c *Coordinator, taskNumber int) {
	// fmt.Println("ENTER FOLLOWSTATUSREDUCE")
	time.Sleep(10 * time.Second)
	c.lock.RLock()
	defer c.lock.RUnlock()
	if _, ok := c.reduceTaskOverview[taskNumber]; ok && c.reduceTaskOverview[taskNumber] != "reduce finished" {
		var tempBucket []string
		for _, intFileName := range c.intermediateFiles {
			bucketIndex := int(intFileName[len(intFileName)-1] - '0')
			if bucketIndex == taskNumber {
				tempBucket = append(tempBucket, intFileName)
			}
		}
		c.buckets <- BucketPackage{tempBucket, taskNumber}
	}
	// fmt.Println("EXITS FOLLOWSTATUSREDUCE")
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// fmt.Println("ENTERS DONE")
	// Your code here.
	/*RETURN TRUE WHEN MAPREDUCE JOB IS COMPLETELY FINISHED*/
	ret := false
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.reduceTaskOverview) == c.nReduce {
		ret = true
		for _, string := range c.reduceTaskOverview {
			if string != "reduce finished" {
				ret = false
			}
		}
	}
	// fmt.Println("DONE RETURNED ", ret)
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filesToMap := make(chan FilesPackage, len(files))
	for index, file := range files {
		filesToMap <- FilesPackage{file, index}
	}
	var lock sync.RWMutex
	buckets := make(chan BucketPackage, nReduce)
	c := Coordinator{
		files,
		filesToMap,
		nReduce,
		make([]string, 0),
		&lock,
		make(map[int]string),
		make(map[int]string),
		buckets,
	}

	// Your code here.
	/*THE MAP PHASE SHOULD DIVIDE THE INTERMEDIATE KEYS INTO BUCKETS FOR NREDUCE REDUCE TASKS*/
	/*ihash(key) % NReduce?*/
	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
