package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}

  // Your declarations here.
  muData sync.RWMutex
  isPrimary bool
  curView viewservice.View
  data    map[string]string
  lastRe  map[int64]string
  failedPings uint8
  needSync bool
}

func (pb *PBServer) put(key string, val string, dohash bool) string {
  preVal := pb.data[key]
  if dohash {
    val = strconv.Itoa(int(hash(preVal + val)))
  }
  pb.data[key] = val
  return preVal
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  if (!pb.isPrimary && args.IsClient) || (pb.isPrimary && !args.IsClient) {
    reply.Err = ErrWrongServer
    return nil
  }

  pb.muData.Lock()
  defer pb.muData.Unlock()
  v, ok := pb.lastRe[args.ReqId]
  if ok {
    reply.PreviousValue = v
    return nil
  }

  previous := pb.put(args.Key, args.Value, args.DoHash)
  pb.lastRe[args.ReqId] = previous
  reply.PreviousValue = previous

  if pb.isPrimary && pb.curView.Backup != "" {
    args.IsClient = false

    backupReply := PutReply{}
    backupPutOk := call(pb.curView.Backup, "PBServer.Put", args, &backupReply)
    if !backupPutOk || (backupReply.Err != ErrWrongServer && backupReply.PreviousValue != previous) {
      syncReply := SyncReply{}
      view := pb.curView
      syncOk := pb.sendSync(view.Backup, &syncReply, false)
      needSync := !syncOk && syncReply.Err != ErrWrongServer
      getViewOk := true
      for needSync && view.Primary == pb.me && view.Backup != "" && getViewOk {
        syncReply = SyncReply{}
        syncOk = pb.sendSync(view.Backup, &syncReply, false)
        needSync = !syncOk && syncReply.Err != ErrWrongServer
        if !needSync {
          break
        }
        view, getViewOk = pb.vs.Get()
      }
    }
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  if (!pb.isPrimary && args.IsClient) || (pb.isPrimary && !args.IsClient) {
    reply.Err = ErrWrongServer
    return nil
  }

  pb.muData.RLock()
  defer pb.muData.RUnlock()
  val, ok := pb.data[args.Key]
  reply.Value = val   // val will be "" if key does not exist
  if !ok {
    reply.Err = ErrNoKey
  }

  if pb.isPrimary && pb.curView.Backup != "" {
    args.IsClient = false

    backupReply := GetReply{}
    backupGetOk := call(pb.curView.Backup, "PBServer.Get", args, &backupReply)
    if !backupGetOk || (backupReply.Err != ErrWrongServer && (backupReply.Value != val || (val == "" && backupReply.Value == "" && backupReply.Err != reply.Err))) {
      syncReply := SyncReply{}
      view := pb.curView
      syncOk := pb.sendSync(view.Backup, &syncReply, false)
      needSync := !syncOk && syncReply.Err != ErrWrongServer
      getViewOk := true
      for needSync && view.Primary == pb.me && view.Backup != "" && getViewOk {
        syncReply = SyncReply{}
        syncOk = pb.sendSync(view.Backup, &syncReply, false)
        needSync = !syncOk && syncReply.Err != ErrWrongServer
        if !needSync {
          break
        }
        view, getViewOk = pb.vs.Get()
      }
    }
  }
  return nil
}

func (pb *PBServer) sendSync(dst string, reply *SyncReply, needLock bool) bool {
  if needLock {
    pb.muData.RLock()
  }

  dataCopy := make(map[string]string)
  for k, v := range pb.data {
    dataCopy[k] = v
  }
  lastReqCopy := make(map[int64]string)
  for k, v := range pb.lastRe {
    lastReqCopy[k] = v
  }

  if needLock {
    pb.muData.RUnlock()
  }

  syncArgs := &SyncArgs{dataCopy, lastReqCopy}
  syncOK := call(dst, "PBServer.ReceiveSync", syncArgs, reply)
  return syncOK
}

// ReceiveSync synchronizes data from the primary node.
func (pb *PBServer) ReceiveSync(syncArgs *SyncArgs, syncReply *SyncReply) error {
  if pb.isPrimary {
   syncReply.Err = ErrWrongServer
  } else {
    pb.muData.Lock()
    pb.data = make(map[string]string)
    for k, v := range syncArgs.Data {
      pb.data[k] = v
    }
    pb.lastRe = make(map[int64]string)
    for k, v := range syncArgs.LastReqs {
      pb.lastRe[k] = v
    }
    pb.muData.Unlock()
  }
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  view, err := pb.vs.Ping(pb.curView.Viewnum)
  if err == nil {
    lastView := pb.curView
    pb.isPrimary = view.Primary == pb.me
    pb.curView = view
    pb.failedPings = 0

    if pb.isPrimary && view.Backup != "" && (lastView.Backup != view.Backup || pb.needSync) {
      syncReply := &SyncReply{}
      syncOK := pb.sendSync(view.Backup, syncReply, true)
      pb.needSync = !syncOK && syncReply.Err != ErrWrongServer
    }

  } else {
    pb.failedPings += 1
    if pb.failedPings == viewservice.DeadPings {
      pb.isPrimary = false
    }
  }
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.curView = viewservice.View{}
  pb.data = make(map[string]string)
  pb.lastRe = make(map[int64]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me)
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
      //fmt.Printf("PB %v is dead? %v\n", pb.me, pb.dead)
    }
    pb.done.Done()
  }()

  return pb
}
