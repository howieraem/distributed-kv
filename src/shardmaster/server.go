package shardmaster

import (
  "encoding/gob"
  "fmt"
  "log"
  "math/rand"
  "net"
  "net/rpc"
  "os"
  "paxos"
  "reflect"
  "sync"
  "syscall"
  "time"
)

const debug = false

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num

  seq int
  idleGroups chan *JoinArgs
}

type OpType int
const (
  Join OpType = iota
  Leave
  Move
  Query
)

type Op struct {
  Type OpType
  Args interface{}
}

func (sm *ShardMaster) dprint(format string, a ...interface{}) (n int, err error) {
  if debug && sm.me == 1 {
    log.Printf(format, a...)
  }
  return
}

func (sm *ShardMaster) getNewCfg() Config {
  preCfg := sm.configs[len(sm.configs) - 1]
  newCfg := Config{}
  newCfg.Num = preCfg.Num + 1
  newCfg.Shards = [NShards]int64{}
  for i := range preCfg.Shards {
    newCfg.Shards[i] = preCfg.Shards[i]
  }
  newCfg.Groups = make(map[int64][]string)
  for gid, srvs := range preCfg.Groups {
    var srvsCopy []string
    for _, srv := range srvs {
      srvsCopy = append(srvsCopy, srv)
    }
    newCfg.Groups[gid] = srvsCopy
  }
  return newCfg
}

func (sm *ShardMaster) handleJoin(args *JoinArgs, newCfg *Config) {
  _, ok := newCfg.Groups[args.GID]
  if ok {
    return
  }
  sm.dprint("[join] before %v %v", newCfg.Shards, args.GID)

  newCfg.Groups[args.GID] = args.Servers

  gidShards := make(map[int64][]int)
  for s, sgid := range newCfg.Shards {
    if sgid != 0 {
      gidShards[sgid] = append(gidShards[sgid], s)
    }
  }
  ng := len(gidShards) + 1
  if ng == 1 {
    // Special case: initially all shards are assigned to null group 0
    for i := range newCfg.Shards {
      newCfg.Shards[i] = args.GID
    }
    return
  } else if ng > NShards {
    // Too many groups, let the new group be a standby
    sm.idleGroups <- args
    return
  }
  div, mod := NShards / ng, NShards % ng
  sm.dprint("[join] debug %v %v %v", ng, div, mod)
  changes := 0
  //for sgid, ss := range gidShards {
  for _, ss := range gidShards {
    n := len(ss)
    for n > div + 1 {
      n--
      newCfg.Shards[ss[n]] = args.GID
      //gidShards[sgid] = ss[:n]
      changes++
    }

    if n > div {
      if mod > 0 {
        mod--
      } else {
        n--
        newCfg.Shards[ss[n]] = args.GID
        //gidShards[sgid] = ss[:n]
        changes++
      }
    }

    if changes == div {
      break
    }
  }
  /*
  var tmp []int
  for _, s := range newCfg.Shards {
    tmp = append(tmp, int(s))
  }
  sort.Ints(tmp)
  sm.dprint("[join] after %v", tmp)
  */
}

func (sm *ShardMaster) handleLeave(args *LeaveArgs, newCfg *Config) {
  _, ok := newCfg.Groups[args.GID]
  if !ok {
    return
  }
  sm.dprint("[leave] before %v %v", newCfg.Shards, args.GID)

  delete(newCfg.Groups, args.GID)

  gidShards := make(map[int64][]int)
  for s, sgid := range newCfg.Shards {
    gidShards[sgid] = append(gidShards[sgid], s)
  }
  _, groupInUse := gidShards[args.GID]
  if !groupInUse {
    return
  }
  ng := len(gidShards) - 1
  if ng == 0 {
    // Special case: no remaining group
    for i := range newCfg.Shards {
      newCfg.Shards[i] = 0
    }
    return
  } else if len(sm.idleGroups) > 0 {
    // Get a group from standbys. If it has not left, do a direct replacement.
    idleJoinArgs := &JoinArgs{}
    exists := false
    for !exists && len(sm.idleGroups) > 0 {
      idleJoinArgs = <- sm.idleGroups
      _, exists = newCfg.Groups[idleJoinArgs.GID]
    }
    if exists {
      for i, sgid := range newCfg.Shards {
        if sgid == args.GID {
          newCfg.Shards[i] = idleJoinArgs.GID
        }
      }
      return
    }
  }

  div, mod := NShards / ng, NShards % ng
  sm.dprint("[leave] debug %v %v %v", ng, div, mod)
  changingShards := gidShards[args.GID]
  ns := len(changingShards)
  delete(gidShards, args.GID)

  for sgid, ss := range gidShards {
    n := len(ss)
    for n < div {
      ns--
      newCfg.Shards[changingShards[ns]] = sgid
      //changingShards = changingShards[:ns]
      n++
    }

    if mod > 0 && n == div {
      ns--
      newCfg.Shards[changingShards[ns]] = sgid
      //changingShards = changingShards[:ns]
      mod--
    }

    if ns == 0 {
      break
    }
  }
  /*
  var tmp []int
  for _, s := range newCfg.Shards {
    tmp = append(tmp, int(s))
  }
  sort.Ints(tmp)
  sm.dprint("[leave] after %v", tmp)
  */
}

func (sm *ShardMaster) mutate(op *Op) {
  if op.Type == Query {
    return
  }

  newCfg := sm.getNewCfg()

  switch op.Type {
  case Join:
    args := op.Args.(JoinArgs)
    sm.handleJoin(&args, &newCfg)
  case Leave:
    args := op.Args.(LeaveArgs)
    sm.handleLeave(&args, &newCfg)
  case Move:
    args := op.Args.(MoveArgs)
    sm.dprint("[move] before %v %v", args.Shard, args.GID)
    newCfg.Shards[args.Shard] = args.GID
    sm.dprint("[move] after %v %v", args.Shard, args.GID)
  default:
    return
  }
  sm.configs = append(sm.configs, newCfg)
}

func (sm *ShardMaster) WaitForAgreement(seq int) interface{} {
  for {
    ok, v := sm.px.Status(seq)
    if ok {
      return v
    }
    time.Sleep(20 * time.Millisecond)
  }
}

func (sm *ShardMaster) agreeOperate(op *Op) {
  seq := sm.seq + 1
  for {
    sm.px.Start(seq, *op)
    agreedOp := sm.WaitForAgreement(seq).(Op)
    if !reflect.DeepEqual(agreedOp, *op) {
      seq++
      continue
    }
    break
  }
  for idx := sm.seq + 1; idx < seq; idx++ {
    v := sm.WaitForAgreement(idx).(Op)
    sm.mutate(&v)
  }
  sm.mutate(op)
  sm.seq = seq
  sm.px.Done(seq)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.agreeOperate(&Op{Type: Join, Args: *args})
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.agreeOperate(&Op{Type: Leave, Args: *args})
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.agreeOperate(&Op{Type: Move, Args: *args})
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.agreeOperate(&Op{Type: Query, Args: *args})
  if args.Num >= 0 && args.Num < len(sm.configs) {
    reply.Config = sm.configs[args.Num]
  } else {
    reply.Config = sm.configs[len(sm.configs) - 1]
  }
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})
  gob.Register(QueryArgs{})
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.idleGroups = make(chan *JoinArgs, 32768)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me])
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
