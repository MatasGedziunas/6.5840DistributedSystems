package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// func VotesNeeded(peerCount int) int {
// 	return peerCount/2 + 1
// }
