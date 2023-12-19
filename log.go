package jsn_raft

type JLog interface {
	Term() uint64
	Index() uint64
	Cmd() []byte
}
