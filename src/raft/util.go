package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func PrintLogEntry(message string, le LogEntry) {
	log.Printf("%v: Command: %v Term: %d Index: %d\n", message, le.Command, le.Term, le.Index)
}

func PrintLogEntries(message string , les []LogEntry) {
	DPrintf("%v 开始打印多行日志信息，共%d行\n", message, len(les))
	for _, le := range les {
		DPrintf("%v: Command: %v Term: %d Index: %d\n", message, le.Command, le.Term, le.Index)
	}
	DPrintf("%v 结束打印多行日志信息，共%d行\n", message, len(les))
}