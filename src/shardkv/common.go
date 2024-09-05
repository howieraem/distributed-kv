package shardkv

import (
  "crypto/rand"
  "hash/fnv"
  "math/big"
  "shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ReConfigOK = "ReConfigOK"
  Dead = "Dead"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ReqId int64
  CliCfgNum int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  ReqId int64
  CliCfgNum int
}

type GetReply struct {
  Err Err
  Value string
}

type OpRes struct {
  K string
  V string
  Err Err
}

type TransferArgs struct {
  Shard int
  CfgNum int
}

type TransferReply struct {
  Err Err
  Data map[string]string
  LastRe map[int64]OpRes
}

type ReConfigArgs struct {
  Cfg shardmaster.Config
  Data map[string]string
  LastRe map[int64]OpRes
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

// Generates unique request id
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
