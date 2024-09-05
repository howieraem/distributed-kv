package pbservice

import (
  "crypto/rand"
  "fmt"
  "math/big"
  "net/rpc"
  "viewservice"
)

// You'll probably need to uncomment these:
// import "time"

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  curPrimary string
  //curView viewservice.View
}


func (ck *Clerk) updatePrimary() {
  ck.curPrimary = ck.vs.Primary()
  //fmt.Println("client refreshed primary:", ck.curPrimary)
  for ck.curPrimary == "" {
    ck.curPrimary = ck.vs.Primary()
  }
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
  args := GetArgs{Key: key, IsClient: true, ReqId: nrand()}
  reply := GetReply{}
  ck.updatePrimary()
  //fmt.Printf("client get first: %v, %v\n", ck.curPrimary, key)
  ok := call(ck.curPrimary, "PBServer.Get", &args, &reply)
  //fmt.Println(ck.curPrimary, ok, reply)
  for !ok || (reply.Err != "" && reply.Err != OK && reply.Err != ErrNoKey) {
    reply = GetReply{}
    ck.updatePrimary()
    //fmt.Printf("client get retry: %v, %v\n", ck.curPrimary, key)
    ok = call(ck.curPrimary, "PBServer.Get", &args, &reply)
  }
  return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  args := PutArgs{Key: key, Value: value, IsClient: true, DoHash: dohash, ReqId: nrand()}
  reply := PutReply{}
  ck.updatePrimary()
  //fmt.Printf("client put first: %v, %v, %v\n", ck.curPrimary, key, value)
  ok := call(ck.curPrimary, "PBServer.Put", &args, &reply)
  for !ok || (reply.Err != "" && reply.Err != OK && reply.Err != ErrNoKey) {
    reply = PutReply{}
    ck.updatePrimary()
    //fmt.Printf("client put retry: %v, %v, %v\n", ck.curPrimary, key, value)
    ok = call(ck.curPrimary, "PBServer.Put", &args, &reply)
  }
  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
