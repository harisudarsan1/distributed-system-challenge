package main

import (
	"context"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
)

type forwader struct {
	incomingMessage <-chan int

	*maelstrom.Node

	sync.Mutex
	neighbours []string
}

// run is a blocking call it will wait for messages or context so to avoid blocking it should be spin up as a go routine
func (f *forwader) run(ctx context.Context) {

	for {

		select {

		case message := <-f.incomingMessage:
			// broadcast to neighbours only
			for _, node := range f.neighbours {
				// send or rpc call to neighbours
				body := map[string]any{}
				body["type"] = "broadcast"
				body["message"] = message
				f.Send(node, body)
			}
			continue

		case <-ctx.Done():
			// exit
			return
		}

	}

}

func (f *forwader) UpdateNeighbours(neighbours []string) {
	f.Lock()
	f.neighbours = neighbours
	f.Unlock()
}

func NewForwarder(ctx context.Context, senderChan <-chan int, node *maelstrom.Node) *forwader {

	forwarder := &forwader{
		incomingMessage: senderChan,
		neighbours:      make([]string, 0),
		Node:            node,
	}

	// return send only channel and forwarder
	return forwarder
}
