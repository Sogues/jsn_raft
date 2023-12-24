package jsn_raft

import (
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ServerConfig struct {
	List []struct {
		Who  string `yaml:"who"`
		Addr string `yaml:"addr"`
	} `yaml:"list"`
}

type RaftNew struct {
	who string

	currentTerm uint64

	serverState int32

	logs []JLog

	logger JLogger

	voteFor string

	config ServerConfig

	goroutineWaitGroup sync.WaitGroup

	rpcChannel chan *rpcWrap
}

func NewRaftNew(who string, config ServerConfig) {
	r := new(RaftNew)
	r.who = who
	r.config = config
	r.logger = new(defaultLogger)
	r.rpcChannel = make(chan *rpcWrap, 256)

	svr := rpc.NewServer()
	if err := svr.RegisterName("RaftNew", r); nil != err {
		panic(err)
	}
	if ls, err := net.Listen("tcp", who); nil != err {
		panic(err)
	} else {
		r.safeGo("accept", func() {
			svr.Accept(ls)
		})
	}
	r.safeGo("fsm", r.fsm)
}

func (r *RaftNew) fsm() {
	for {
		switch r.getServerState() {
		case follower:
			r.runFollower()
		case candidate:
			r.runCandidate()
		case leader:
			r.runLeader()

		}
	}
}

func (r *RaftNew) safeGo(name string, f func()) {
	r.goroutineWaitGroup.Add(1)
	go func() {
		defer func() {
			if err := recover(); nil != err {
				r.logger.Error("safe go panic %v recover %v", name, err)
			}
			r.goroutineWaitGroup.Done()
		}()
		f()
	}()
}

func (r *RaftNew) lastLog() (lastLogIndex, lastLogTerm uint64) {
	if 0 == len(r.logs) {
		return
	}
	log := r.logs[len(r.logs)-1]
	return log.Index(), log.Term()
}

func randomTimeout(interval time.Duration) time.Duration {
	return interval + time.Duration(rand.Int63())%interval
}

func (r *RaftNew) getServerState() int32 {
	return atomic.LoadInt32(&r.serverState)
}

func (r *RaftNew) getCurrentTerm() uint64 {
	return r.currentTerm
}

func (r *RaftNew) setServerState(state int32) {
	old := atomic.SwapInt32(&r.serverState, state)
	r.logger.Info("[%v] state from %v to %v",
		r.who, old, state)
}

func (r *RaftNew) setCurrentTerm(term uint64) {
	old := atomic.SwapUint64(&r.currentTerm, term)
	r.logger.Debug("[%v] term from %v to %v",
		r.who, old, term)
}

func (r *RaftNew) addCurrentTerm() uint64 {
	ret := atomic.AddUint64(&r.currentTerm, 1)
	r.logger.Debug("[%v] term from %v to %v",
		r.who, ret-1, ret)
	return ret
}

func (r *RaftNew) heartbeatTimeout() time.Duration {
	return time.Second
}

func (r *RaftNew) electionTimeout() time.Duration {
	return time.Second
}

func (r *RaftNew) rpcTimeout() time.Duration {
	return time.Second
}

func (r *RaftNew) runFollower() {

	timeout := time.After(randomTimeout(r.heartbeatTimeout()))

	for r.getServerState() == follower {
		select {
		case wrap := <-r.rpcChannel:
			r.rpc(wrap)
		case <-timeout:
			r.setServerState(candidate)
			return
		}
	}
}

func (r *RaftNew) runCandidate() {
	r.logger.Info("[%v] in candidate",
		r.who)
	r.addCurrentTerm()

	r.voteFor = r.who
	lastLogIndex, lastLogTerm := r.lastLog()
	req := &VoteRequest{
		Term:         r.getCurrentTerm(),
		CandidateId:  []byte(r.who),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var (
		needVotes    = len(r.config.List)/2 + 1
		grantedVotes = 0
	)

	voteResponseChannel := make(chan *VoteResponse, len(r.config.List)-1)
	for _, v := range r.config.List {
		who := v.Who
		if who == r.who {
			grantedVotes++
			r.voteFor = r.who
		} else {
			r.safeGo("peer vote request", func() {
				resp := &VoteResponse{}
				rpcCli := r.getRpc(who)
				if nil != rpcCli {
					// todo 处理泄露
					r.logger.Debug("[%v] request vote from %v",
						r.who, who)
					select {
					case <-time.After(r.rpcTimeout()):
					case call := <-rpcCli.Go(Vote, req, resp, nil).Done:
						if nil != call.Error {
							r.logger.Error("[%v] vote to %v err %v",
								r.who, who, call.Error.Error())
						}

					}
					rpcCli.Close()
				}
				voteResponseChannel <- resp
			})
		}
	}

	electionTimer := time.After(randomTimeout(r.electionTimeout()))

	for r.getServerState() == candidate {
		select {
		case wrap := <-r.rpcChannel:
			r.rpc(wrap)
		case resp := <-voteResponseChannel:
			if resp.CurrentTerm > r.getCurrentTerm() {
				r.setServerState(follower)
				r.setCurrentTerm(resp.CurrentTerm)
				return
			}
			if resp.VoteGranted {
				grantedVotes++
				r.logger.Debug("[%v] receive vote, current votes %v need %v",
					r.who, grantedVotes, needVotes)
			}
			if grantedVotes >= needVotes {
				r.setServerState(leader)
			}
		case <-electionTimer:
			// 重新开始投票
			r.logger.Debug("[%v] current votes %v need %v timeout",
				r.who, grantedVotes, needVotes)
			return
		}
	}

}

func (r *RaftNew) runLeader() {
	r.logger.Info("[%v] in leader",
		r.who)
	tk := time.After(time.Second * 3)
	for r.getServerState() == leader {
		select {
		case <-tk:
			r.setServerState(follower)
		}
	}
}

type rpcWrap struct {
	In   any
	Resp any
	Done chan error
}

func (r *RaftNew) getRpc(who string) *rpc.Client {
	// todo
	dial, err := net.Dial("tcp", who)
	if nil != err {
		r.logger.Error("[%v] get rpc error from %v err %v",
			r.who, who, err)
		return nil
	}
	cli := rpc.NewClient(dial)
	return cli
}

func (r *RaftNew) Vote(args *VoteRequest, reply *VoteResponse) error {
	wrap := &rpcWrap{
		In:   args,
		Resp: reply,
		Done: make(chan error),
	}
	r.rpcChannel <- wrap
	err := <-wrap.Done
	//reply.CurrentTerm, reply.VoteGranted = r.vote(args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	//rpcInfo(args, reply, r.log, fmt.Sprintf("who:%v rpc Vote", r.addr))
	return err
}

func (r *RaftNew) rpc(wrap *rpcWrap) {
	switch tp := wrap.In.(type) {
	case *VoteRequest:
		err := r.vote(tp, wrap.Resp.(*VoteResponse))
		wrap.Done <- err
	}
}

func (r *RaftNew) vote(args *VoteRequest, reply *VoteResponse) error {
	if args.Term < r.getCurrentTerm() {
		return nil
	} else if args.Term > r.getCurrentTerm() {
		r.setCurrentTerm(args.Term)
		r.setServerState(follower)
		r.voteFor = ""
	}
	reply.CurrentTerm = r.getCurrentTerm()
	reply.VoteGranted = false

	if 0 != len(r.voteFor) && r.voteFor == string(args.CandidateId) {
		reply.VoteGranted = true
		return nil
	}

	lastLogIndex, lastLogTerm := r.lastLog()
	if lastLogTerm > args.LastLogTerm {
		return nil
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		return nil
	}
	reply.VoteGranted = true
	r.voteFor = string(args.CandidateId)
	r.logger.Debug("[%v] vote for %v term %v",
		r.who, string(args.CandidateId), r.getCurrentTerm())
	return nil

}
