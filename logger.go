package jsn_raft

import "fmt"

type JLogger interface {
	Error(format string, params ...any)
	Info(format string, params ...any)
	Panic(format string, params ...any)
	Debug(format string, params ...any)
}

type defaultLogger struct {
}

func (d defaultLogger) Debug(format string, params ...any) {
	fmt.Printf("[Debug]"+format+"\n", params...)
}

func (d defaultLogger) Panic(format string, params ...any) {
	panic(fmt.Errorf(format, params...))
}

func (d defaultLogger) Info(format string, params ...any) {
	fmt.Printf("[Info]"+format+"\n", params...)
}

func (d defaultLogger) Error(format string, params ...any) {
	fmt.Printf("[Error]"+format+"\n", params...)
}
