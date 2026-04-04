package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	_, cancel := context.WithCancel(context.Background())

	n := maelstrom.NewNode()

	// logs := map[string][]string{}
	lm := NewLogManager()

	n.Handle("send", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key, ok := body["key"].(string)
		if !ok {
			return fmt.Errorf("key should be string")
		}

		msgValue, ok := body["msg"].(float64)
		if !ok {
			return fmt.Errorf("msgValue should be float64")
		}

		offset := lm.Send(key, int(msgValue))

		body["type"] = "send_ok"
		delete(body, "key")
		delete(body, "msg")
		body["offset"] = offset

		return n.Reply(msg, body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var offsets map[string]any
		var ok bool
		offsets, ok = body["offsets"].(map[string]any)

		pollmsgMap := make(map[string]int, 0)
		if ok && offsets != nil {

			for k, v := range offsets {
				keyoffset, ok := v.(float64)
				if ok {
					pollmsgMap[k] = int(keyoffset)
				}
			}

		}

		body["msgs"] = lm.Poll(pollmsgMap)
		body["type"] = "poll_ok"
		delete(body, "offsets")

		return n.Reply(msg, body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var offsets map[string]any
		var ok bool
		offsets, ok = body["offsets"].(map[string]any)

		CommitmsgMap := make(map[string]int, 0)
		if ok && offsets != nil {

			for k, v := range offsets {
				keyoffset, ok := v.(float64)
				if ok {
					CommitmsgMap[k] = int(keyoffset)
				}
			}

		}

		lm.Commit(CommitmsgMap)
		body["type"] = "commit_offsets_ok"
		delete(body, "offsets")

		return n.Reply(msg, body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		logkeys, ok := body["keys"].([]any)

		keys := []string{}
		if ok {
			for _, logkey := range logkeys {

				key, ok := logkey.(string)
				if !ok {
					return fmt.Errorf("key %v is not a string", logkey)
				}
				keys = append(keys, key)
			}
		}
		offsetsMap :=	lm.ListCommitedOffsets(keys)

		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = offsetsMap
		delete(body, "keys")

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// var topology map[string]any
		// var ok bool
		// topology, ok = body["topology"].(map[string]any)
		// if ok && topology != nil {
		// 	// topology exists so get the neighbours and update the forwarder
		// 	rawNeighbours, ok := topology[n.ID()].([]any)
		// 	if ok {
		// 		neighbours := make([]string, 0, len(rawNeighbours))
		// 		for _, v := range rawNeighbours {
		// 			if s, ok := v.(string); ok {
		// 				neighbours = append(neighbours, s)
		// 			}
		// 		}
		// 		forwarder.UpdateNeighbours(neighbours)
		// 	}
		// }

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	// Ignore broadcast_ok replies from other nodes
	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
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
