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

// how often to resend unacknowledged messages
var gossipResendRate = 1 * time.Second

type syncIntSet struct {
	mu   sync.Mutex
	vals map[int]struct{}
}

func newSyncIntSet() *syncIntSet {
	return &syncIntSet{mu: sync.Mutex{}, vals: make(map[int]struct{})}
}

func (s *syncIntSet) Append(x int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals[x] = struct{}{}
}

func (s *syncIntSet) Contains(x int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.vals[x]
	return ok
}

func (s *syncIntSet) Remove(x int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vals, x)
}

type Gossiper struct {
	n            *maelstrom.Node
	nextAckID    int
	mu           sync.Mutex
	outbox       map[string]chan int
	awaitingAcks map[string]*syncIntSet
}

func NewGossiper(n *maelstrom.Node) *Gossiper {
	outbox := make(map[string]chan int)
	awaitingAcks := make(map[string]*syncIntSet)
	nextAckID := 1
	return &Gossiper{n, nextAckID, sync.Mutex{}, outbox, awaitingAcks}
}

func (g *Gossiper) Gossip(dest string, val int) {
	g.mu.Lock()
	_, ok := g.outbox[dest]
	if !ok {
		g.outbox[dest] = make(chan int)
		g.awaitingAcks[dest] = newSyncIntSet()
		go g.startGossipWithDest(dest)
	}

	outbox := g.outbox[dest]
	g.mu.Unlock()

	outbox <- val
}

func (g *Gossiper) startGossipWithDest(dest string) {
	g.mu.Lock()
	outbox := g.outbox[dest]
	g.mu.Unlock()

	ticker := time.NewTicker(gossipBatchRate)
	buf := make([]int, 0)
	for {
		select {
		case v, ok := <-outbox:
			if !ok {
				if len(buf) > 0 {
					go g.sendBatch(dest, buf)
				}
				close(outbox)
				ticker.Stop()
				return
			}
			buf = append(buf, v)
		case <-ticker.C:
			if len(buf) > 0 {
				go g.sendBatch(dest, buf)
			}
			buf = nil
		}
	}
}

func (g *Gossiper) sendBatch(dest string, vals []int) {
	g.mu.Lock()
	awaitingAcks := g.awaitingAcks[dest]
	awaitingAcks.Append(g.nextAckID)
	ackID := g.nextAckID
	body := make(map[string]any)
	body["type"] = "gossip"
	body["messages"] = vals
	body["ack_id"] = ackID
	g.nextAckID++
	g.mu.Unlock()
	for {
		if !awaitingAcks.Contains(ackID) {
			// this message has already been acked, do not re-send
			return
		}
		err := g.n.Send(dest, body)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		time.Sleep(gossipResendRate)
	}

}

func (g *Gossiper) Ack(dest string, ackID int) {
	// remove ackID from awaitingAcks
	g.mu.Lock()
	g.awaitingAcks[dest].Remove(ackID)
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
		ackID, ok := body["ack_id"].(float64)
		if !ok {
			return errors.New("missing ack_id in gossip")
		}

		mu.Lock()
		defer mu.Unlock()

		for _, val := range vals {
			if !slices.Contains(messages, int(val.(float64))) {
				messages = append(messages, int(val.(float64)))
			}
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

		g.Ack(msg.Src, int(ackID))
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
