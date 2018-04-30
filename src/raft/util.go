package raft

import "log"
import "time"
// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func max(x int, y int) (int) {
	if x >= y {
		return x
	} else {
		return y
	}
}

func min(x int, y int) (int) {
	if x >= y {
		return y
	} else {
		return x
	}
}

func mins(xs []int) (int) {
	// make sure len(xs) > 0
	minv := xs[0] // or raise panic
	for i:=1;i<len(xs);i++{
		minv = min(minv, xs[i])
	}
	return minv
}

func signalTimeout(ch chan int, timeout time.Duration) {
	select {
	case <-time.After(timeout):
	case ch<-1:
	}
}

func counts(xs []int, f func(x int) bool) int {
	count := 0
	for i:=0; i<len(xs); i++ {
		if f(xs[i]) {
			count++
		}
	}
	return count
}

const (
	SUCCESS	int  = iota
	CANDIDATE_TIMEOUT	
	FOLLOWER_TIMEOUT
	LEADER_TIMEOUT
	RESET	
)