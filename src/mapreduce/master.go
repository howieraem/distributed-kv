package mapreduce

import (
  "container/list"
  "fmt"
  "sync"
)

type WorkerInfo struct {
  address string
  // You can add definitions here.
  //avail bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

// GetWorker returns the address of an available worker
func (mr *MapReduce) GetWorker() string {
  workerAddr, avail := "", false
  for workerAddr == "" {
    mr.mu.Lock()
    for workerAddr, avail = range mr.WorkerAvail {
      if avail {
        //mr.WorkerAvail[workerAddr] = false
        delete(mr.WorkerAvail, workerAddr)  // worker will be in use, remove it from container
        mr.mu.Unlock()
        return workerAddr
      }
    }
    mr.mu.Unlock()  // allows updating from registration channel
  }
  return workerAddr
}

// RunJobs allocates jobs to workers
func (mr *MapReduce) RunJobs(op JobType) {
  var nJobs, nOther int
  if op == Map {
    nJobs, nOther = mr.nMap, mr.nReduce
  } else {
    nJobs, nOther = mr.nReduce, mr.nMap
  }

  var wg sync.WaitGroup
  wg.Add(nJobs)

  for i := 0; i < nJobs; i++ {
    go func(idx int) {
      arg := DoJobArgs{
        File: mr.file,
        Operation: op,
        JobNumber: idx,
        NumOtherPhase: nOther}
      re := DoJobReply{false}

      workerAddr := mr.GetWorker()
      for {
        ok := call(workerAddr, "Worker.DoJob", &arg, &re)
        if ok && re.OK {
          mr.mu.Lock()
          // Put worker back to the container. The size limit of a map is usually larger than a channel.
          mr.WorkerAvail[workerAddr] = true
          mr.mu.Unlock()
          break
        }

        // Find another available worker
        workerAddr = mr.GetWorker()
      }
      wg.Done()
    }(i)
  }
  wg.Wait() // Wait until all goroutines above finish
}

func (mr *MapReduce) RunMaster() *list.List {
  go func() {
    // Keep reading new workers
    for {
      workerAddr := <- mr.registerChannel
      mr.mu.Lock()
      mr.WorkerAvail[workerAddr] = true
      mr.mu.Unlock()
    }
  }()

  mr.RunJobs(Map)
  mr.RunJobs(Reduce)
  return mr.KillWorkers()
}
