package viewservice

import (
  "fmt"
  "log"
  "math"
  "net"
  "net/rpc"
  "os"
  "sync"
  "time"
)

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  ack bool
  curView, tmpView View
  pingTime map[string]int64
  idleServers chan string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vn := args.Viewnum
  ser := args.Me
  vs.mu.Lock()
  defer vs.mu.Unlock()

  if vn == 0 {
    // Rebooted or new KV server
    if _, ok := vs.pingTime[ser]; ok {
      // Existing KV server rebooted
      if vs.ack {
        // Current primary ACKed
        if ser == vs.curView.Primary {
          // Current primary rebooted
          if vs.curView.Backup != "" {
            // Promote the current backup
            vs.tmpView.Primary = vs.curView.Backup
            vs.tmpView.Backup = ser
            vs.tmpView.Viewnum = vs.curView.Viewnum + 1
            vs.curView = vs.tmpView
            vs.ack = false
          } else {
            // No current backup
          }
        } else if ser == vs.curView.Backup {
          // Current backup rebooted, get a new one from idle servers if any
          if len(vs.idleServers) > 0 {
            vs.tmpView = vs.curView
            vs.tmpView.Backup = <- vs.idleServers
            vs.tmpView.Viewnum++
            vs.ack = false
          }
        } else if vs.curView.Backup == "" {
          // Current backup is missing, use the rebooted KV server
          vs.tmpView = vs.curView
          vs.tmpView.Backup = ser
          vs.tmpView.Viewnum++
          vs.ack = false
        }
      } else {
        // Current primary not yet ACKed, will not change view
      }

    } else {
      // New KV server
      if vs.curView.Primary == "" {
        // View service just initialized
        vs.tmpView.Primary = ser
        vs.tmpView.Viewnum++
        vs.curView = vs.tmpView
        vs.ack = false
      } else if vs.curView.Backup == "" {
        // Backup is missing
        vs.tmpView.Primary = vs.curView.Primary
        vs.tmpView.Backup = ser
        vs.tmpView.Viewnum = vs.curView.Viewnum + 1
        vs.ack = false
      } else if !vs.ack && vs.tmpView.Backup == "" {
        // View service is waiting for primary's ACK and doesn't have a backup
        vs.tmpView.Backup = ser
        vs.tmpView.Viewnum++
      } else {
        // Both primary and backup have been taken, mark it idle
        if len(vs.idleServers) < cap(vs.idleServers) {
          vs.idleServers <- ser
        }
      }
    }

  } else {
    if !vs.ack && ser == vs.tmpView.Primary && vn == vs.tmpView.Viewnum {
      // ACK Ping from the primary
      vs.curView = vs.tmpView
      vs.tmpView = View{}
      vs.ack = true
    }
  }
  vs.pingTime[ser] = time.Now().UnixNano()
  if !vs.ack {
    reply.View = vs.tmpView
  } else {
    reply.View = vs.curView
  }

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  if !vs.ack {
    reply.View = vs.tmpView
  } else {
    reply.View = vs.curView
  }
  return nil
}

func (vs *ViewServer) isDead(ser string) bool {
  return float64(time.Now().UnixNano() - vs.pingTime[ser]) / float64(PingInterval) >= DeadPings
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  if vs.ack {
    if vs.isDead(vs.curView.Primary) {
      if vs.curView.Backup != "" {
        vs.curView.Primary = vs.curView.Backup
        vs.curView.Backup = ""
        vs.curView.Viewnum++
        vs.tmpView = vs.curView
        vs.ack = false
      } else {
        // No backup, KV service failed
      }
    } else if vs.isDead(vs.curView.Backup) {
      if len(vs.idleServers) > 0 {
        vs.ack = false
        vs.curView.Backup = <- vs.idleServers
        vs.curView.Viewnum++
        vs.tmpView = vs.curView
      } else {
        //vs.ack = false
        //vs.curView.Viewnum++
        vs.curView.Backup = ""
        //vs.tmpView = vs.curView
      }
    }
  } // else: Keep waiting for primary's ACK without changing the view

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.pingTime = make(map[string]int64)
  vs.idleServers = make(chan string, math.MaxUint16)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me)
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
