package jsn_raft

type JLogger interface {
	Error(format string, params ...any)
}
