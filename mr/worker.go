package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Your worker implementation here.
		reply, err := CallRequestTask()
		if err != nil {
			return
		}

		if reply.TaskType == "map" {
			// fmt.Println("MAP WORKER ", reply.MapTask.MapTaskNumber)
			// fmt.Printf("reply.MapTask.FileName %v\n", reply.MapTask.FileName)
			/*BORROWED CODE FROM MRSEQUENTIAL.GO*/
			file, err := os.Open(reply.MapTask.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapTask.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapTask.FileName)
			}
			file.Close()
			/*MAP FUNCTION RETURNS A LIST OF KEY-VALUE PAIRS STORED AS []KEYVALUE*/
			kva := mapf(reply.MapTask.FileName, string(content))
			// fmt.Printf("mapf kva %v\n", kva)
			/*CREATES [] OF []KEYVALUE, WITH NREDUCE NUMBER OF BUCKETS, AKA SLICE OF SLICE*/
			partitionBuckets := make([][]KeyValue, reply.NReduce)
			/*ITERATES OVER PAIRS IN KVA*/
			for _, pair := range kva {
				/*DETERMINED BUCKET NUMBER BY HASH AND MODULO BUCKETS*/
				bucketIndex := ihash(pair.Key) % reply.NReduce
				/*ADDS PAIR TO PARTITION BUCKET*/
				/*APPEND ADDS PAIR TO PARTITION BUCKET, THEN REASSIGN TO CURRENT PARTITION BUCKET
				APPEND MAY CREATE A NEW UNDERLYING DATA STRUCTURE IF PARTITION BUCKET NEEDS TO BE BIGGER*/
				partitionBuckets[bucketIndex] = append(partitionBuckets[bucketIndex], pair)
			}
			// fmt.Println("partitionBuckets ", partitionBuckets)
			/*ITERATE OVER PARTITION BUCKETS*/
			intFiles := make([]string, reply.NReduce)
			for i, partition := range partitionBuckets {
				/*CREATE AN INTERMEDIATE FILE FOR THE PARTITION
				FOLLOWING RECOMMENDED FORMAT MR-X-Y, X IS MAP TASK NUMBER AND Y IS REDUCE TASK NUMBER*/
				/*HARDCODING MAP TASK NUMBER TO 0 FOR NOW WHILE USING c.filesToMap[0]*/
				// fmt.Println("PARTITION ", i)
				intFile := fmt.Sprintf("mr-%v-%v", reply.MapTask.MapTaskNumber, i)
				file, err := os.Create(intFile)
				if err != nil {
					fmt.Println("Error: ", err)
				}
				intFiles[i] = intFile
				/*fILE WILL CLOSE AFTER THIS WHOLE BLOCK EXITS*/
				defer file.Close()
				/*USING RECOMMENDED encoding/json PACKAGE, BORROWED CODE FROM README.MD*/
				enc := json.NewEncoder(file)
				for _, pair := range partition {
					err := enc.Encode(&pair)
					if err != nil {
						fmt.Println("Error: ", err)
					}
				}
			}
			CallReturnMapResult(reply.MapTask, intFiles)
		} else if reply.TaskType == "reduce" {
			// fmt.Println("REDUCE ", reply.ReduceTask.ReduceTaskNumber, " STARTED")
			bucket := reply.ReduceTask.Bucket
			kvmap := make(map[string][]string)
			/*OPEN FILES IN BUCKET*/
			for _, fileName := range bucket {
				file, err := os.Open(fileName)
				if err != nil {
					fmt.Printf("cannot open file: %v\n", fileName)
				}
				defer file.Close()
				/*USING RECOMMENDED encoding/json PACKAGE, BORROWED CODE FROM README.MD*/
				/*OBTAINS KEY VALUE PAIRS FROM FILE*/
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
				}
			}
			/*GENERATE OUTPUT FILE*/
			outputFile := fmt.Sprintf("mr-out-%v", reply.ReduceTask.ReduceTaskNumber)
			file, err := os.Create(outputFile)
			// fmt.Println("GENERATED OUTPUT FILE ", reply.ReduceTask.ReduceTaskNumber)
			if err != nil {
				fmt.Printf("error creating output file: %v\n", err)
				return
			}
			defer file.Close()
			/*ADD KEY LIST PAIRS TO FILE*/
			for key, list := range kvmap {
				output := reducef(key, list)
				/*CORRECT FORMAT FROM MRSQUENTIAL.GO*/
				fmt.Fprintf(file, "%v %v\n", key, output)
			}
			CallReturnReduceResult(reply.ReduceTask.ReduceTaskNumber)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func CallRequestTask() (ReplyTask, error) {
	args := Empty{}
	reply := ReplyTask{}
	success := call("Coordinator.RequestTask", &args, &reply)
	if !success {
		return reply, errors.New("Coordinator is unreachable")
	}
	return reply, nil
}

func CallReturnMapResult(mapTask MapTask, intFiles []string) {
	args := MapResult{
		MapTask:  mapTask,
		IntFiles: intFiles,
	}
	reply := Empty{}
	call("Coordinator.ReturnMapResult", &args, &reply)
}

func CallReturnReduceResult(reduceTaskNumber int) {
	args := reduceTaskNumber
	reply := Empty{}
	call("Coordinator.ReturnReduceResult", args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
