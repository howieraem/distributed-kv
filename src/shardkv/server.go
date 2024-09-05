package shardkv

import (
  "encoding/gob"
  "fmt"
  "log"
  "math/rand"
  "net"
  "net/rpc"
  "os"
  "paxos"
  "shardmaster"
  "strconv"
  "sync"
  "syscall"
  "time"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

const MaxTransferRetries = 3000

type OpType int
const (
  Get OpType = iota
  Put
  ReConfig
  Sync
)

type Op struct {
  // Your definitions here.
  Type OpType
  Id int64
  Args interface{}
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  seq int
  data map[string]string
  lastRe map[int64]OpRes
  curCfg shardmaster.Config
}

func (kv *ShardKV) copyCfg(cfg *shardmaster.Config) shardmaster.Config {
  newCfg := shardmaster.Config{}
  newCfg.Num = cfg.Num
  newCfg.Shards = [shardmaster.NShards]int64{}
  for i := range cfg.Shards {
    newCfg.Shards[i] = cfg.Shards[i]
  }
  newCfg.Groups = make(map[int64][]string)
  for gid, srvs := range cfg.Groups {
    var srvsCopy []string
    for _, srv := range srvs {
      srvsCopy = append(srvsCopy, srv)
    }
    newCfg.Groups[gid] = srvsCopy
  }
  return newCfg
}

func (kv *ShardKV) HandleTransfer(args *TransferArgs, reply *TransferReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  if !kv.agreeOperate(&Op{Type: Sync, Id: nrand()}) {
    reply.Err = Dead
    return nil
  }
  if args.CfgNum >= kv.curCfg.Num {
    reply.Err = ErrWrongGroup
    return nil
  }
  DPrintf("transfer data in shard %v", args.Shard)

  reply.Data = make(map[string]string)
  for k, v := range kv.data {
    if args.Shard == key2shard(k) {
      reply.Data[k] = v
    }
  }

  reply.LastRe = make(map[int64]OpRes)
  for k, v := range kv.lastRe {
    //if args.Shard == key2shard(v.K) || v.Err == ReConfigOK {
      reply.LastRe[k] = v
    //}
  }

  reply.Err = OK
  return nil
}

func (kv *ShardKV) requestData(cfg *shardmaster.Config, shard int, outputChan chan<- *TransferReply) {
  srvs, exists := cfg.Groups[cfg.Shards[shard]]
  if !exists || len(srvs) == 0 {
    outputChan <- &TransferReply{}
    return
  }

  retries := 0
  args := &TransferArgs{shard, cfg.Num}
  doneChan := make(chan bool, 1)

  for {
    nWrongGroup := 0

    for _, srv := range srvs {
      reply := &TransferReply{}
      
      go func() {
        doneChan <- call(srv, "ShardKV.HandleTransfer", args, reply)
      }()
      var ok bool
      select {
        case ok = <-doneChan:
        case <-time.After(5 * time.Second):
          outputChan <- &TransferReply{}
          return
      }

      if ok {
        if reply.Err == OK {
          outputChan <- reply
          return
        } else if reply.Err == ErrWrongGroup {
          nWrongGroup++
        }
      }

      retries++
      if kv.dead || retries >= MaxTransferRetries {
        outputChan <- &TransferReply{}
        return
      }
      //DPrintf("retry retrieving data from another group, %v %v", ok, reply.Err)
    }

    if nWrongGroup == len(srvs) {
      // Skip this transfer
      outputChan <- &TransferReply{}
      return
    }
  }
}

func (kv *ShardKV) mutate(op *Op) bool {
  if _, exists := kv.lastRe[op.Id]; exists {
    return true
  }
  switch op.Type {
  case Get:
    args := op.Args.(GetArgs)
    if args.CliCfgNum != kv.curCfg.Num {
      return false
    }
    v, ok := kv.data[args.Key]
    res := OpRes{K: args.Key}
    if ok {
      res.V = v
      res.Err = OK
    } else {
      res.Err = ErrNoKey
    }
    kv.lastRe[args.ReqId] = res
  case Put:
    args := op.Args.(PutArgs)
    if args.CliCfgNum != kv.curCfg.Num {
      return false
    }
    res := OpRes{K: args.Key}
    preVal := kv.data[args.Key]
    val := args.Value
    if args.DoHash {
      res.V = preVal
      val = strconv.Itoa(int(hash(preVal + val)))
    }
    kv.data[args.Key] = val
    res.Err = OK
    kv.lastRe[args.ReqId] = res
  case ReConfig:
    args := op.Args.(ReConfigArgs)
    if args.Cfg.Num <= kv.curCfg.Num {
      kv.lastRe[op.Id] = OpRes{Err: ReConfigOK}
      return true
    }

    kv.curCfg = kv.copyCfg(&(args.Cfg))

    for k, v := range args.Data {
      kv.data[k] = v
    }

    for k, v := range args.LastRe {
      kv.lastRe[k] = v
    }

    kv.lastRe[op.Id] = OpRes{Err: ReConfigOK}
  }
  return true
}

func (kv *ShardKV) waitForAgreement(seq int) interface{} {
  for !kv.dead {
    ok, v := kv.px.Status(seq)
    //DPrintf("seq: %v, v: %v, ok: %v", seq, v, ok)
    if ok {
      return v
    }
    time.Sleep(20 * time.Millisecond)
  }
  return nil
}

func (kv *ShardKV) agreeOperate(op *Op) bool {
  seq := kv.seq + 1
  for {
    if _, exists := kv.lastRe[op.Id]; exists {
      return true
    }
    kv.px.Start(seq, *op)
    v := kv.waitForAgreement(seq)
    if v == nil {
      return false
    }
    if v.(Op).Id != op.Id {
      seq++
      continue
    }
    break
  }
  for idx := kv.seq + 1; idx < seq; idx++ {
    v := kv.waitForAgreement(idx)
    if v == nil {
      return false
    }
    preOp := v.(Op)
    kv.mutate(&preOp)
  }
  res := kv.mutate(op)
  kv.seq = seq
  kv.px.Done(seq)
  return res
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Get %v", args.Key)
  if !kv.agreeOperate(&Op{Type: Sync, Id: nrand()}) {
    reply.Err = Dead
    return nil
  }

  if kv.agreeOperate(&Op{Get, args.ReqId, *args}) {
    res := kv.lastRe[args.ReqId]
    reply.Value = res.V
    reply.Err = res.Err
    DPrintf("Get reply %v", *reply)
  } else {
    reply.Err = ErrWrongGroup
  }

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Put args %v %v %v", args.Key, args.Value, args.ReqId)
  if !kv.agreeOperate(&Op{Type: Sync, Id: nrand()}) {
    reply.Err = Dead
    return nil
  }

  if kv.agreeOperate(&Op{Put, args.ReqId, *args}) {
    res := kv.lastRe[args.ReqId]
    reply.PreviousValue = res.V
    reply.Err = res.Err
    DPrintf("Put reply %v", *reply)
  } else {
    reply.Err = ErrWrongGroup
  }

  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()

  kv.agreeOperate(&Op{Type: Sync, Id: nrand()})
  cfg := kv.sm.Query(kv.curCfg.Num + 1)
  if cfg.Num != kv.curCfg.Num {
    kv.agreeOperate(&Op{Type: Sync, Id: nrand()})
    preCfg := kv.copyCfg(&(kv.curCfg))
    kv.mu.Unlock()

    transData := make(map[string]string)
    transLastRe := make(map[int64]OpRes)
    doneChan := make(chan *TransferReply, shardmaster.NShards)
    differentShards := 0
    for i := 0; i < shardmaster.NShards; i++ {
      if preCfg.Shards[i] != kv.gid && preCfg.Shards[i] != 0 && cfg.Shards[i] == kv.gid {
        go func(idx int) {
          kv.requestData(&preCfg, idx, doneChan)
        }(i)
        differentShards++
      }
    }

    for ; differentShards > 0; differentShards-- {
      reply := <-doneChan
      if reply.Err != OK || kv.dead {
        return
      }
      for k, v := range reply.Data {
        transData[k] = v
      }
      for k, v := range reply.LastRe {
        transLastRe[k] = v
      }
    }

    args := ReConfigArgs{cfg, transData, transLastRe}
    kv.mu.Lock()
    kv.agreeOperate(&Op{ReConfig, nrand(), args})
  }
  kv.mu.Unlock()
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetArgs{})
  gob.Register(PutArgs{})
  gob.Register(ReConfigArgs{})
  gob.Register(shardmaster.Config{})
  gob.Register(OpRes{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.data = make(map[string]string)
  kv.lastRe = make(map[int64]OpRes)
  kv.curCfg = shardmaster.Config{Num: -1}

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
