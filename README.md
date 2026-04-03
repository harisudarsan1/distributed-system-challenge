# My solutions for fly.io distributed systems challenges
I have written every piece of code by myself no ai agents are used for writing the code as this is for brainstroming purposes.

To test it 

1. Install dependencies to run ./maelstrom. `brew install openjdk graphviz gnuplot`
2. Setup maelstrom version 0.2.3 by downloading from [link](https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.3)
3. clone this repo 
4. do `go build ./...`
5. then run maelstrom commands for example 

```bash
./maelstrom test -w broadcast --bin ./maelstrom-echo --node-count 5 --time-limit 20 --rate 10
```

## Challenge Tracker

| # | Challenge | Status |
|---|-----------|--------|
| 1 | Echo | Done |
| 2 | Unique ID Generation | Done |
| 3a | Broadcast: Single-Node | Done |
| 3b | Broadcast: Multi-Node | Done |
| 3c | Broadcast: Fault Tolerant | Done |
| 3d | Broadcast: Efficient, Part I | Done |
| 3e | Broadcast: Efficient, Part II | Done |
| 4 | Grow-Only Counter | Done |
| 5a | Kafka-Style Log: Single-Node | Pending |
| 5b | Kafka-Style Log: Multi-Node | Pending |
| 5c | Kafka-Style Log: Efficient | Pending |
| 6a | Transactions: Single-Node | Pending |
| 6b | Transactions: Read Uncommitted | Pending |
| 6c | Transactions: Read Committed | Pending |

