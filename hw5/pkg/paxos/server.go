package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) Majority() int {
	return (len(server.peers) >> 1) + 1
}

func (server *Server) setPhase(phase string) {
	server.proposer.Phase = phase
	server.proposer.ResponseCount = 0
	server.proposer.SuccessCount = 0
	for i := range server.proposer.Responses {
		server.proposer.Responses[i] = false
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	nPeers := len(server.peers)
	majority := server.Majority()
	var res []base.Node

	switch msg := message.(type) {
	case *ProposeRequest:
		newNode := server.copy()
		response := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(msg.To(), msg.From()),
			Peer: newNode.me,
			SessionId: msg.SessionId,
		}
		if !base.IsNil(newNode.agreedValue) {
			response.AgreedV = newNode.agreedValue
		} else if msg.N > newNode.n_p {
			newNode.n_p = msg.N
			response.Ok = true
			response.N_a = newNode.n_a
			response.V_a = newNode.v_a
		} else {
			response.N_p = newNode.n_p
		}

		newNode.SetSingleResponse(response)
		res = append(res, newNode)
	case *ProposeResponse:
		if server.proposer.Phase == Propose && msg.SessionId == server.proposer.SessionId && !server.proposer.Responses[msg.Peer] {
			newNode := server.copy()
			newNode.proposer.Responses[msg.Peer] = true
			newNode.proposer.ResponseCount++
			if msg.Ok {
				newNode.proposer.SuccessCount++
				if msg.N_a > newNode.proposer.N_a_max {
					newNode.proposer.N_a_max = msg.N_a
					newNode.proposer.V = msg.V_a
				}
			} else if !base.IsNil(msg.AgreedV) {
				/*
				// Enter the "Decide" phase directly
				newNode.setPhase(Decide)
				decides := make([]base.Message, nPeers)
				for i, addr := range newNode.peers {
					decideRequest := &DecideRequest{
						CoreMessage: base.MakeCoreMessage(newNode.Address(), addr),
						SessionId:   newNode.proposer.SessionId,
						V:           msg.AgreedV,
					}
					decides[i] = decideRequest
				}
				newNode.SetResponse(decides)
				*/
				newNode.v_a = msg.AgreedV
				newNode.agreedValue = msg.AgreedV
				res = append(res, newNode)
				break
			}

			if newNode.proposer.SuccessCount >= majority {
				if newNode.proposer.ResponseCount <= nPeers {
					// Continue waiting
					newNodeWaiting := newNode.copy()
					res = append(res, newNodeWaiting)
				}

				// Enter the "Accept" phase
				newNode.setPhase(Accept)
				accepts := make([]base.Message, nPeers)
				for i, addr := range newNode.peers {
					acceptRequest := &AcceptRequest{
						CoreMessage: base.MakeCoreMessage(newNode.Address(), addr),
						N: newNode.proposer.N,
						SessionId: newNode.proposer.SessionId,
						V: newNode.proposer.V,
					}
					accepts[i] = acceptRequest
				}
				newNode.SetResponse(accepts)
				res = append(res, newNode)
			} else if newNode.proposer.ResponseCount == nPeers {
				// Received responses from all peers but majority not reached, backoff

				// Case 1: wait a bit
				newNodeWaiting := newNode.copy()
				res = append(res, newNodeWaiting)

				// Case 2: re-propose
				newNodes := newNode.TriggerTimer()
				res = append(res, newNodes...)
			} else {
				// Waiting for more responses
				res = append(res, newNode)
			}
		}
	case *AcceptRequest:
		newNode := server.copy()
		response := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(msg.To(), msg.From()),
			Peer: newNode.me,
			SessionId: msg.SessionId,
		}
		if !base.IsNil(newNode.agreedValue) {
			response.AgreedV = newNode.agreedValue
		} else if msg.N >= newNode.n_p {
			newNode.n_p = msg.N
			newNode.n_a = msg.N
			newNode.v_a = msg.V
			response.Ok = true
			response.N_p = msg.N
		}
		newNode.SetSingleResponse(response)
		res = append(res, newNode)
	case *AcceptResponse:
		if server.proposer.Phase == Accept && msg.SessionId == server.proposer.SessionId && !server.proposer.Responses[msg.Peer] {
			newNode := server.copy()
			newNode.proposer.Responses[msg.Peer] = true
			newNode.proposer.ResponseCount++
			if msg.Ok {
				newNode.proposer.SuccessCount++
			} else if !base.IsNil(msg.AgreedV) {
				/*
				// Enter the "Decide" phase directly
				newNode.setPhase(Decide)
				decides := make([]base.Message, nPeers)
				for i, addr := range newNode.peers {
					decideRequest := &DecideRequest{
						CoreMessage: base.MakeCoreMessage(newNode.Address(), addr),
						SessionId: newNode.proposer.SessionId,
						V: msg.AgreedV,
					}
					decides[i] = decideRequest
				}
				newNode.SetResponse(decides)
				*/
				newNode.v_a = msg.AgreedV
				newNode.agreedValue = msg.AgreedV
				res = append(res, newNode)
				break
			}

			if newNode.proposer.SuccessCount >= majority {
				if newNode.proposer.ResponseCount <= nPeers {
					// Continue waiting
					newNodeWaiting := newNode.copy()
					res = append(res, newNodeWaiting)
				}

				// Enter the "Decide" phase
				newNode.setPhase(Decide)
				decides := make([]base.Message, nPeers)
				for i, addr := range newNode.peers {
					decideRequest := &DecideRequest{
						CoreMessage: base.MakeCoreMessage(newNode.Address(), addr),
						SessionId: newNode.proposer.SessionId,
						V: newNode.proposer.V,
					}
					decides[i] = decideRequest
				}
				newNode.SetResponse(decides)
				res = append(res, newNode)
			} else if newNode.proposer.ResponseCount == nPeers {
				// Received responses from all peers but majority not reached, backoff

				// Case 1: wait a bit
				newNodeWaiting := newNode.copy()
				res = append(res, newNodeWaiting)

				// Case 2: re-propose
				newNodes := newNode.TriggerTimer()
				res = append(res, newNodes...)
			} else {
				// Waiting for more responses
				res = append(res, newNode)
			}
		}
	case *DecideRequest:
		if base.IsNil(server.agreedValue) {
			newNode := server.copy()
			newNode.v_a = msg.V
			newNode.agreedValue = msg.V
			res = append(res, newNode)
		}
	default:
	}
	return res
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	server.setPhase(Propose)
	server.proposer.SessionId++
	server.proposer.N++
	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue

	proposes := make([]base.Message, len(server.peers))
	for i, addr := range server.peers {
		proposeRequest := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), addr),
			N: server.proposer.N,
			SessionId: server.proposer.SessionId,
		}
		proposes[i] = proposeRequest
	}
	server.SetResponse(proposes)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
