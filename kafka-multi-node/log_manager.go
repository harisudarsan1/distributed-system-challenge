package main

import (
	"context"
	"sync"
    "encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


type Log struct {
    Messages         []int `json:"messages"`
    LastCommitOffset int   `json:"lastCommitOffset"`
    TotalLen         int   `json:"totalLen"`
}

// ToJSON converts Log to JSON string
func (l Log) ToJSON() (string, error) {
    bytes, err := json.Marshal(l)
    if err != nil {
        return "", err
    }
    return string(bytes), nil
}

// ToBytes converts Log to JSON bytes
func (l Log) ToBytes() ([]byte, error) {
    return json.Marshal(l)
}

// FromJSON parses JSON string into Log
func (l *Log) FromJSON(jsonStr string) error {
    return json.Unmarshal([]byte(jsonStr), l)
}

// FromBytes parses JSON bytes into Log
func (l *Log) FromBytes(data []byte) error {
    return json.Unmarshal(data, l)
}

// FromMap converts map[string]any to Log (useful for KV store results)
func (l *Log) FromMap(data map[string]any) error {
    bytes, err := json.Marshal(data)
    if err != nil {
        return err
    }
    return json.Unmarshal(bytes, l)
}

type LogManager struct {
	sync.Mutex
	logs map[string]Log
	 *maelstrom.KV
}


// Send will receive the message and returns the offset of the stored message
func (lm *LogManager) Send(key string, message int) int {
	lm.Lock()
	defer lm.Unlock()
	// logg, ok := lm.logs[key]

	ctx := context.Background()

	val,err := lm.KV.Read(ctx, key)
	if err != nil {
		logImpl := Log{
			Messages:         []int{message},
			LastCommitOffset: 0,
			TotalLen:         1,
		}
		lm.Write(ctx, key, logImpl)
		return 0
	}


    // Parse existing log via JSON
    var logg Log
    jsonBytes, err := json.Marshal(val)
    if err != nil {
        return -1
    }
    if err := json.Unmarshal(jsonBytes, &logg); err != nil {
        return -1
    }


	offset := len(logg.Messages)

	logg.Messages = append(logg.Messages, message)
	logg.TotalLen = offset + 1
	lm.Write(ctx, key, logg)

	return offset
}

func (lm *LogManager) Poll(pollMsgs map[string]int) map[string][][]int {

	plReplies := make(map[string][][]int, len(pollMsgs))

	for key, offset := range pollMsgs {
		lm.Lock()
		logg, ok := lm.logs[key]
		lm.Unlock()

		if !ok {
			// plr := PollReply{
			// 	key:               pm.key,
			// 	messageWithOffset: [][]int{},
			// }

			plReplies[key] = [][]int{}

		} else {
			msgsWithOffset := make([][]int, 0)
			// msgs := logg.messages[pm.offset:]
			// add the offset+message value in the array
			for i := offset; i < logg.TotalLen; i++ {
				msgsWithOffset = append(msgsWithOffset, []int{i, logg.Messages[i]})
			}
			// plr := PollReply{
			// 	key:               pm.key,
			// 	messageWithOffset: msgsWithOffset,
			// }
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
			logg.LastCommitOffset = offset
			lm.logs[key] = logg
		}

		lm.Unlock()
	}

}

func (lm *LogManager) ListCommitedOffsets(keys []string) map[string]int {
	commitmap := make(map[string]int, len(keys))
	lm.Lock()
	for k, logg := range lm.logs {
		commitmap[k] = logg.LastCommitOffset
	}
	lm.Unlock()
	return commitmap
}

func NewLogManager(kv *maelstrom.KV) *LogManager {
	logs := make(map[string]Log, 50)

	logMgr := &LogManager{
		logs: logs,
		KV: kv,
	}

	return logMgr

}
