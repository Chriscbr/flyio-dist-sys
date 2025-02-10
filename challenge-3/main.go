package main

import (
	"encoding/json"
	"errors"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// how often to send out batches of gossip
var gossipBatchRate = 1 * time.Second

type Gossiper struct {
	n         *maelstrom.Node
	mu        sync.Mutex
	outbox    map[string][]int
	ackedVals map[string][]int
}

func NewGossiper(n *maelstrom.Node) *Gossiper {
	outbox := make(map[string][]int)
	ackedVals := make(map[string][]int)
	return &Gossiper{n, sync.Mutex{}, outbox, ackedVals}
}

func (g *Gossiper) Gossip(dest string, val int) {
	g.mu.Lock()
	_, ok := g.outbox[dest]
	if !ok {
		g.outbox[dest] = []int{}
		g.ackedVals[dest] = []int{}
		go g.startGossipWithDest(dest)
	}
	g.outbox[dest] = append(g.outbox[dest], val)
	g.mu.Unlock()
}

func (g *Gossiper) startGossipWithDest(dest string) {
	ticker := time.NewTicker(gossipBatchRate)
	for range ticker.C {
		g.mu.Lock()
		unackedVals := make([]int, 0)
		for _, val := range g.outbox[dest] {
			if !slices.Contains(g.ackedVals[dest], val) {
				unackedVals = append(unackedVals, val)
			}
		}
		g.outbox[dest] = unackedVals
		if len(unackedVals) > 0 {
			body := make(map[string]any)
			body["type"] = "gossip"
			body["messages"] = unackedVals
			err := g.n.Send(dest, body)
			if err != nil {
				log.Printf("Error: %v", err)
			}
		}
		g.mu.Unlock()
	}
}

func (g *Gossiper) Ack(dest string, vals ...int) {
	g.mu.Lock()
	g.ackedVals[dest] = append(g.ackedVals[dest], vals...)
	g.mu.Unlock()
}

func main() {
	n := maelstrom.NewNode()

	mu := sync.Mutex{}  // guards `messages`
	messages := []int{} // all messages received to date

	g := NewGossiper(n)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val, ok := body["message"].(float64)
		if !ok {
			return errors.New("non-number message received in broadcast")
		}

		mu.Lock()
		defer mu.Unlock()

		if !slices.Contains(messages, int(val)) {
			messages = append(messages, int(val))
			for _, n2 := range n.NodeIDs() {
				if n2 == n.ID() {
					continue
				}
				g.Gossip(n2, int(val))
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		vals, ok := body["messages"].([]any)
		if !ok {
			return errors.New("non-array messages received in gossip")
		}

		mu.Lock()
		defer mu.Unlock()

		for _, val := range vals {
			if !slices.Contains(messages, int(val.(float64))) {
				messages = append(messages, int(val.(float64)))
			}
		}

		return n.Send(msg.Src, map[string]any{"type": "gossip_ok", "messages": vals})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		vals, ok := body["messages"].([]any)
		if !ok {
			return errors.New("non-array messages received in gossip_ok")
		}

		valsInt := make([]int, 0)
		for _, val := range vals {
			valsInt = append(valsInt, int(val.(float64)))
		}
		g.Ack(msg.Src, valsInt...)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
