package main

import (
	"context"
	"encoding/json"
	"log"

	"fmt"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	// buffered channel to handle backpressure
	messageChan := make(chan int, 100)

	n := maelstrom.NewNode()

	broadcastMessageNums := []int{}

	forwarder := NewForwarder(ctx, messageChan, n)
	// broadcaster
	go forwarder.run(ctx)

	broadcastmessagebucket := Newbroadcastmessagebucket(messageChan)

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgType, ok := body["type"].(string)
		if !ok {
			return fmt.Errorf("invalid or missing type field")
		}

		switch msgType {
		case "broadcast":
			message := body["message"]
			if message != nil {
				if num, ok := message.(float64); ok {
					broadcastMessageNums = append(broadcastMessageNums, int(num))
					broadcastmessagebucket.AddMessage(int(num))
				}
			}
			body["type"] = "broadcast_ok"
			delete(body, "message")

		case "read":
			body["type"] = "read_ok"
			// body["messages"] = broadcastMessageNums
			body["messages"] = broadcastmessagebucket.GetAllMessages()

		case "topology":
			body["type"] = "topology_ok"

		default:
			return fmt.Errorf("unknown message type: %s", msgType)
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = broadcastmessagebucket.GetAllMessages()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var topology map[string]any
		var ok bool
		topology, ok = body["topology"].(map[string]any)
		if ok && topology != nil {
			// topology exists so get the neighbours and update the forwarder
			rawNeighbours, ok := topology[n.ID()].([]any)
			if ok {
				neighbours := make([]string, 0, len(rawNeighbours))
				for _, v := range rawNeighbours {
					if s, ok := v.(string); ok {
						neighbours = append(neighbours, s)
					}
				}
				forwarder.UpdateNeighbours(neighbours)
			}
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	// Ignore broadcast_ok replies from other nodes
	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = uuid.NewString()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"
		// body.Type = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)

	})

	if err := n.Run(); err != nil {
		cancel()
		log.Fatal(err)
	}

	cancel()

}

// func getNeighBourNodes()
