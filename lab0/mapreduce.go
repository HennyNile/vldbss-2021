package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string   // only for map, the input file
	phase      jobPhase // are we in mapPhase or reducePhase?
	taskNumber int      // this task's index in the current phase
	nMap       int      // number of map tasks
	nReduce    int      // number of reduce tasks
	mapF       MapF     // map function used in this job
	reduceF    ReduceF  // reduce function used in this job
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

// GetMRCluster returns a reference to a MRCluster.
func GetMRCluster() *MRCluster {
	return singleton
}

// NWorkers returns how many workers there are in this cluster.
func (c *MRCluster) NWorkers() int { return c.nWorkers }

// Start starts this cluster.
func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				// fmt.Println("mapPhase:", t.dataDir, t.jobName, t.taskNumber)
				content, err := ioutil.ReadFile(t.mapFile)
				if err != nil {
					panic(err)
				}

				fs := make([]*os.File, t.nReduce)
				bs := make([]*bufio.Writer, t.nReduce)
				for i := range fs {
					rpath := reduceName(t.dataDir, t.jobName, t.taskNumber, i)
					fs[i], bs[i] = CreateFileAndBuf(rpath)
				}
				results := t.mapF(t.mapFile, string(content))
				for _, kv := range results {
					enc := json.NewEncoder(bs[ihash(kv.Key)%t.nReduce])
					if err := enc.Encode(&kv); err != nil {
						log.Fatalln(err)
					}
				}
				for i := range fs {
					SafeClose(fs[i], bs[i])
				}
			} else if t.phase == reducePhase {
				// fmt.Println("reducePhase:", t.dataDir, t.jobName, t.taskNumber)
				
				// read data from tmp file of map phase
				contents := make([][]byte, t.nMap)
				for i := 0; i < t.nMap; i++ {
					rpath := reduceName(t.dataDir, t.jobName, i, t.taskNumber)
					content, err := ioutil.ReadFile(rpath)
					if err != nil {
						panic(err)
					}
					contents[i] = content
				}

				// create tmp file for reduce phase 
				var bs *bufio.Writer
				respath := mergeName(t.dataDir, t.jobName, t.taskNumber) 
				_, bs = CreateFileAndBuf(respath)

				// reduce result of map
				//var stringValues []string
				var lines [][]byte
				var kv KeyValue
				reduceMap := make(map[string][]string)
				for _, cont := range contents{
					lines = bytes.Split(cont, []byte("\n"))
					lines = lines[:len(lines)-1]
					// fmt.Println(len(lines), "records in file")
					for _, l := range lines {
						json.Unmarshal(l, &kv)
						if len(kv.Key+kv.Value) != 0 {
							reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
						}
					}
				}
	
				// run reduce function and output result
				// sort.Strings(reduceMapKeys)
				for k, v := range reduceMap {
					results := t.reduceF(k, v)
					bs.Write([]byte(results))
				}
				bs.Flush()								
			}
			t.wg.Done()
		case <-c.exit:
			return
		}
	}
}

// Shutdown shutdowns this cluster.
func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

// Submit submits a job to this cluster.
func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	// map phase
	nMap := len(mapFiles)
	mapTasks := make([]*task, 0, nMap)
	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			mapF:       mapF,
		}
		t.wg.Add(1)
		mapTasks = append(mapTasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range mapTasks {
		t.wg.Wait()
	}

	// reduce phase
	reduceTasks := make([]*task, 0, nReduce)
	var respaths []string
	for j := 0; j < nReduce; j++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			phase:      reducePhase,
			taskNumber: j,
			nReduce:    nReduce,
			nMap:       nMap,
			reduceF:    reduceF,
		}
		t.wg.Add(1)
		reduceTasks = append(reduceTasks, t)
		go func() { c.taskCh <- t }()
		respaths = append(respaths, mergeName(dataDir, jobName, j))
	}
	for _, t := range reduceTasks {
		t.wg.Wait()
	}
	notify <- respaths
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
