package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.V == "v1"
	}

	a2RejP1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		if s1.proposer.Phase == Accept {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.From() == s2.Address() && m.To() == s1.Address() {
					return true
				}
			}
		}
		return false
	}

	return []func(s *base.State) bool {p1PreparePhase, p3PreparePhase, p1AcceptPhase, a2RejP1}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.V == "v3"
	}

	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide && s3.proposer.V == "v3"
	}

	s3Consensus := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.agreedValue == "v3"
	}

	return []func(s *base.State) bool {p3AcceptPhase, p3DecidePhase, s3Consensus}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Propose && s1.proposer.SuccessCount >= s1.Majority() {
			fmt.Println("... p1 entering Accept phase the 1st time")
			return true
		}
		return false
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase == Propose && s3.proposer.SuccessCount >= s3.Majority() {
			fmt.Println("... p3 entering Accept phase the 1st time")
			return true
		}
		return false
	}

	p1AcceptRejected := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Accept {
			seen := make(map[base.Address]bool)
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() {
					seen[m.From()] = true
				}
			}
			if len(seen) >= s1.Majority() {
				fmt.Println("... p1's Accept phase getting rejected the 1st time")
				return true
			}
		}
		return false
	}

	return []func(s *base.State) bool {p1PreparePhase, p1AcceptPhase, p3PreparePhase, p3AcceptPhase, p1AcceptRejected}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p1PreparePhase2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.N > s1.n_p
	}

	p1AcceptPhase2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Propose && s1.proposer.SuccessCount >= s1.Majority() {
			fmt.Println("... p1 entering Accept phase the 2nd time")
			return true
		}
		return false
	}

	p3AcceptRejected := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase == Accept {
			seen := make(map[base.Address]bool)
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s3.Address() {
					seen[m.From()] = true
				}
			}
			if len(seen) >= s3.Majority() {
				fmt.Println("... p3's Accept phase getting rejected the 1st time")
				return true
			}
		}
		return false
	}

	return []func(s *base.State) bool {p1PreparePhase2, p1AcceptPhase2, p3AcceptRejected}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p3PreparePhase2 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s3.n_p
	}

	p3AcceptPhase2 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase == Propose && s3.proposer.SuccessCount >= s3.Majority() {
			fmt.Println("... p3 entering Accept phase the 2nd time")
			return true
		}
		return false
	}

	p1AcceptRejected2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Accept {
			seen := make(map[base.Address]bool)
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() {
					seen[m.From()] = true
				}
			}
			if len(seen) >= s1.Majority() {
				fmt.Println("... p1's Accept phase getting rejected the 2nd time")
				return true
			}
		}
		return false
	}

	return []func(s *base.State) bool {p3PreparePhase2, p3AcceptPhase2, p1AcceptRejected2}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Propose && s1.proposer.V == "v1" && s1.proposer.SuccessCount >= s1.Majority() {
			fmt.Println("... p1 entering Accept phase")
			return true
		}
		return false
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase == Propose && s3.proposer.V == "v3" && s3.proposer.SuccessCount >= s3.Majority() {
			fmt.Println("... p3 entering Accept phase")
			return true
		}
		return false
	}

	p1AcceptRejected := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Accept {
			seen := make(map[base.Address]bool)
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() {
					seen[m.From()] = true
				}
			}
			if len(seen) >= s1.Majority() {
				fmt.Println("... p1's Accept phase getting rejected")
				return true
			}
		}
		return false
	}

	return []func(s *base.State) bool {p1PreparePhase, p1AcceptPhase, p3PreparePhase, p3AcceptPhase, p1AcceptRejected}
}

// Fill in the function to lead the program continue P3's proposal and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase == Accept && s3.proposer.V == "v3" && s3.proposer.SuccessCount >= s3.Majority() {
			fmt.Println("... p3 entering Decide phase")
			return true
		}
		return false
	}

	p3MajorityDecided := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.agreedValue == "v3" && (s1.agreedValue == s3.agreedValue || s2.agreedValue == s3.agreedValue)
	}

	return []func(s *base.State) bool {p3DecidePhase, p3MajorityDecided}
}
