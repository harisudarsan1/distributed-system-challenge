package main

type broadcastmessagebucket struct{
	messages map[int]int
}


func (bmb *broadcastmessagebucket) AddMessage(message int){
	_, ok := bmb.messages[message]
	if ok {
   // exists ignore
		return
	}
	bmb.messages[message] = 1
	// call forwarder
}

func (bmb *broadcastmessagebucket) GetAllMessages() []int {
	messages := []int{}

	for k,_ := range bmb.messages{
	messages = append(messages, k)
	}

	// return all messages for read
	return messages
}

func Newbroadcastmessagebucket() *broadcastmessagebucket {
     bucket := &broadcastmessagebucket{
			messages: make(map[int]int),
	}
   return bucket
}
