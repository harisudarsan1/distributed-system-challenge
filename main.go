package main

import (
    "encoding/json"
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
     "github.com/google/uuid"
	"fmt"

)



func main() {


n := maelstrom.NewNode()

broadcastMessageNums := []int{}

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
							}
			}
				body["type"] = "broadcast_ok"
			delete(body, "message")
			

			case "read":
				body["type"] = "read_ok"
				body["messages"] = broadcastMessageNums

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
		body["messages"] = broadcastMessageNums

    return n.Reply(msg, body)
	})
n.Handle("topology", func(msg maelstrom.Message) error {

// Unmarshal the message body as an loosely-typed map.
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    body["type"] = "topology_ok"
		delete(body,"topology")

    return n.Reply(msg, body)
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
    log.Fatal(err)
}



}
