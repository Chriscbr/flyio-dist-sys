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

type outmsg struct {
	dest  string
	ackID int
	body  map[string]any
}

type Gossiper struct {
	n            *maelstrom.Node
	nextAckID    int
	outbox       chan outmsg
	awaitingAcks []int
}

func NewGossiper(n *maelstrom.Node) *Gossiper {
	outbox := make(chan outmsg)
	awaitingAcks := make([]int, 0)
	nextAckID := 1
	return &Gossiper{n, nextAckID, outbox, awaitingAcks}
}

func (g *Gossiper) Start() {
	go g.sendMessages()
}

func (g *Gossiper) Gossip(dest string, message any) {
	ackID := g.nextAckID
	body := make(map[string]any)
	body["type"] = "gossip"
	body["message"] = message
	body["ack_id"] = ackID
	g.awaitingAcks = append(g.awaitingAcks, ackID)
	g.outbox <- outmsg{dest, ackID, body}
	g.nextAckID++
}

func (g *Gossiper) Ack(ackID int) {
	// remove ackID from awaitingAcks
	_ = slices.DeleteFunc(g.awaitingAcks, func(ackID2 int) bool {
		return ackID2 == ackID
	})
}

func (g *Gossiper) sendMessages() {
	for o := range g.outbox {
		if !slices.Contains(g.awaitingAcks, o.ackID) {
			// this message has already been acked, do not re-send
			continue
		}
		err := g.n.Send(o.dest, o.body)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		go func() {
			time.Sleep(1 * time.Second)
			g.outbox <- o
		}()
	}
}

func main() {
	n := maelstrom.NewNode()

	mu := sync.Mutex{}  // guards `messages`
	messages := []int{} // all messages received to date

	g := NewGossiper(n)
	g.Start()

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
				g.Gossip(n2, val)
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val, ok := body["message"].(float64)
		if !ok {
			return errors.New("non-number message received in gossip")
		}
		ackID, ok := body["ack_id"].(float64)
		if !ok {
			return errors.New("missing ack_id in gossip")
		}

		mu.Lock()
		defer mu.Unlock()

		if !slices.Contains(messages, int(val)) {
			messages = append(messages, int(val))
		}

		return n.Send(msg.Src, map[string]any{"type": "gossip_ok", "ack_id": ackID})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ackID, ok := body["ack_id"].(float64)
		if !ok {
			return errors.New("missing ack_id in gossip_ok")
		}

		g.Ack(int(ackID))
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
