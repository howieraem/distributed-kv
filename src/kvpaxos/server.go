package kvpaxos

import (
  "net"
  "strconv"
  "time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type OpType string
const (
  GET = "GET"
  PUT = "PUT"
  PUTHASH = "PUTHASH"
)

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type OpType
  Key, Value string
  ReqId int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  seq int
  data map[string]string
  lastRe map[int64]string
}

func (kv *KVPaxos) mutate(op *Op) {
  if op.Type == GET {
    return
  }

  preVal := kv.data[op.Key]
  val := op.Value
  if op.Type == PUTHASH {
    kv.lastRe[op.ReqId] = preVal
    val = strconv.Itoa(int(hash(preVal + val)))
  } else {
    kv.lastRe[op.ReqId] = ""
  }

  kv.data[op.Key] = val
  DPrintf("[DEBUG] DB after put(%v, %v): %v", op.Key, val, kv.data)
}

func (kv *KVPaxos) agreeOperate(op *Op) bool {
  to := 10 * time.Millisecond
  started := false
  for !kv.dead {
    decided, decidedV := kv.px.Status(kv.seq)
    DPrintf("[DEBUG] %v: Decision for seq %v: %v, %v", kv.me, kv.seq, decided, decidedV)
    if decided {
      decidedOp := decidedV.(Op)
      kv.mutate(&decidedOp)
      if op.ReqId == decidedOp.ReqId {
        kv.px.Done(kv.seq)
        kv.seq++
        return true
      }
      kv.seq++
      DPrintf("[DEBUG] Request %v not yet handled. Retry in next round.", op.ReqId)
      started = false
      to = 10 * time.Millisecond
    } else {
      if !started {
        kv.px.Start(kv.seq, *op)
        DPrintf("[DEBUG] %v: Operation %v started with seq %v", kv.me, *op, kv.seq)
        started = true
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
  return false
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  op := &Op{ReqId: args.ReqId, Type: GET, Key: args.Key}
  if kv.agreeOperate(op) {
    v, ok := kv.data[args.Key]
    if ok {
      reply.Err = OK
      reply.Value = v
    } else {
      reply.Err = ErrNoKey
    }
  } else {
    reply.Err = ServerDead
  }
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  v, ok := kv.lastRe[args.ReqId]
  if ok {
    reply.Err = OK
    reply.PreviousValue = v
    return nil
  }

  var opType OpType
  if args.DoHash {
    opType = PUTHASH
  } else {
    opType = PUT
  }
  op := &Op{ReqId: args.ReqId, Type: opType, Key: args.Key, Value: args.Value}

  if kv.agreeOperate(op) {
    reply.Err = OK
    reply.PreviousValue = kv.lastRe[args.ReqId]
  } else {
    reply.Err = ServerDead
  }
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.data = make(map[string]string)
  kv.lastRe = make(map[int64]string)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me])
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

