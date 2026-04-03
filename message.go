package main

import "sync"

type broadcastmessagebucket struct {
	sync.Mutex
	messages      map[int]int
	broadcastChan chan<- int
}

func (bmb *broadcastmessagebucket) AddMessage(message int) {
	bmb.Lock()
	_, ok := bmb.messages[message]
	if ok {
		// exists ignore
	  bmb.Unlock()
		return
	}
	bmb.messages[message] = 1
	// release the lock before sending the message
	bmb.Unlock()
	// send message to forwarder which broadcasts requests
	bmb.broadcastChan <- message
}

func (bmb *broadcastmessagebucket) GetAllMessages() []int {
	messages := []int{}

	for k, _ := range bmb.messages {
		messages = append(messages, k)
	}

	// return all messages for read RPC
	return messages
}

func Newbroadcastmessagebucket(broadcastChan chan<- int) *broadcastmessagebucket {
	bucket := &broadcastmessagebucket{
		messages:      make(map[int]int),
		broadcastChan: broadcastChan,
	}
	return bucket
}
