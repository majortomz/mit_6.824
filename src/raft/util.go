package raft

import (
	"log"
	"time"
)

// Debugging
const LEVEL = Disable

const (
	Trace   int = 1
	Debug   int = 2
	Info    int = 3
	Warn    int = 4
	Error   int = 5
	Disable int = 1000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug >= LEVEL {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Trace >= LEVEL {
		log.Printf(format, a...)
	}
	return
}

func InfoPrintf(format string, a ...interface{}) (n int, err error) {
	if Info >= LEVEL {
		log.Printf(format, a...)
	}
	return
}

func WarnPrintf(format string, a ...interface{}) (n int, err error) {
	if Warn >= LEVEL {
		log.Printf(format, a...)
	}
	return
}

func ErrorPrintf(format string, a ...interface{}) (n int, err error) {
	if Error >= LEVEL {
		log.Printf(format, a...)
	}
	return
}

func CurrentMilliSeconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
