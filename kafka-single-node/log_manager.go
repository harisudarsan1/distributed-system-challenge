package main

import (
	"sync"
)


type Log struct {
	messages         []int
	lastCommitOffset int
	totalLen         int
}

type LogManager struct {
	sync.Mutex
	logs map[string]Log
}


// Send will receive the message and returns the offset of the stored message
func (lm *LogManager) Send(key string, message int) int {
	lm.Lock()
	defer lm.Unlock()
	logg, ok := lm.logs[key]
	if !ok {
		// If key is not initialized we'll initilize it and then add the element
		logImpl := Log{
			messages:         []int{message},
			lastCommitOffset: 0,
			totalLen:         1,
		}

		lm.logs[key] = logImpl
		// 0 will be the initial offset
		return 0

	}

	offset := len(logg.messages)

	logg.messages = append(logg.messages, message)
	logg.totalLen = offset + 1
	lm.logs[key] = logg

	return offset
}

func (lm *LogManager) Poll(pollMsgs map[string]int) map[string][][]int {

	plReplies := make(map[string][][]int, len(pollMsgs))

	for key, offset := range pollMsgs {
		lm.Lock()
		logg, ok := lm.logs[key]
		lm.Unlock()

		if !ok {

			plReplies[key] = [][]int{}

		} else {
			msgsWithOffset := make([][]int, 0)
			for i := offset; i < logg.totalLen; i++ {
				msgsWithOffset = append(msgsWithOffset, []int{i, logg.messages[i]})
			}
			plReplies[key] = msgsWithOffset

		}
	}

	return plReplies
}

func (lm *LogManager) Commit(cmgs map[string]int) {
	for key, offset := range cmgs {
		lm.Lock()

		logg, ok := lm.logs[key]
		if ok {
			logg.lastCommitOffset = offset
			lm.logs[key] = logg
		}

		lm.Unlock()
	}

}

func (lm *LogManager) ListCommitedOffsets(keys []string) map[string]int {
	commitmap := make(map[string]int, len(keys))
	lm.Lock()
	for k, logg := range lm.logs {
		commitmap[k] = logg.lastCommitOffset
	}
	lm.Unlock()
	return commitmap
}

func NewLogManager() *LogManager {
	logs := make(map[string]Log, 50)

	logMgr := &LogManager{
		logs: logs,
	}

	return logMgr

}
