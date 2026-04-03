package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := body["delta"].(float64)
		oldvalue, err := kv.Read(ctx, n.ID())
		if err != nil {
			kv.Write(ctx, n.ID(), int(delta))
		}

		value, ok := oldvalue.(int)
		if !ok {
			return fmt.Errorf("Error parsing old value")
		}
		value = value + int(delta)
		kv.Write(ctx, n.ID(), value)

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		totalvalue := 0

	for _,nodeId := range n.NodeIDs(){
		oldvalue, err := kv.Read(ctx, nodeId)
			if err != nil{
        continue
			}

		value, ok := oldvalue.(int)
		if !ok {
			return fmt.Errorf("Error parsing old value")
		}
			totalvalue += value
		}

		body["type"] = "read_ok"
		body["value"] = totalvalue

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
