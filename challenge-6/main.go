package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// how often to send out batches of gossip
var gossipBatchRate = 1 * time.Second

type Gossiper struct {
	n       *maelstrom.Node
	mu      sync.Mutex
	pending map[string][]GossipInfo
}

type GossipInfo struct {
	Id     string  `json:"id"`
	Writes [][]int `json:"writes"`
}

func NewGossiper(n *maelstrom.Node) *Gossiper {
	return &Gossiper{n, sync.Mutex{}, make(map[string][]GossipInfo)}
}

func (g *Gossiper) Gossip(txn []Op) {
	var writes [][]int
	for _, op := range txn {
		if op.OpType == "w" {
			writes = append(writes, []int{op.Key, *op.Value})
		}
	}

	// don't gossip if there are no writes
	if len(writes) == 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for _, n := range g.n.NodeIDs() {
		if n == g.n.ID() { // don't gossip to yourself
			continue
		}
		g.pending[n] = append(g.pending[n], GossipInfo{uuid.NewString(), writes})
	}
}

func (g *Gossiper) startGossip() {
	ticker := time.NewTicker(gossipBatchRate)
	defer ticker.Stop()
	for range ticker.C {
		g.mu.Lock()
		for _, n := range g.n.NodeIDs() {
			if n != g.n.ID() && len(g.pending[n]) > 0 {
				g.sendGossip(n)
			}
		}
		g.mu.Unlock()
	}
}

func (g *Gossiper) sendGossip(n string) {
	err := g.n.Send(n, map[string]any{"type": "gossip", "gossip": g.pending[n]})
	if err != nil {
		log.Printf("error: %v", err)
	}
}

func (g *Gossiper) Ack(dest, id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// filter out gossip with the given id
	remaining := make([]GossipInfo, 0)
	for _, gossip := range g.pending[dest] {
		if gossip.Id != id {
			remaining = append(remaining, gossip)
		}
	}
	g.pending[dest] = remaining
}

type Store struct {
	mu   sync.Mutex
	data map[int]int
}

type Op struct {
	OpType string
	Key    int
	Value  *int
}

func (op *Op) UnmarshalJSON(data []byte) error {
	var raw [3]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || len(raw) != 3 {
		return errors.New("invalid op format")
	}

	if err := json.Unmarshal(raw[0], &op.OpType); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &op.Key); err != nil {
		return err
	}
	if string(raw[2]) != "null" { // check for null before unmarshaling
		var v int
		if err := json.Unmarshal(raw[2], &v); err != nil {
			return err
		}
		op.Value = &v
	}
	return nil
}

func (c Op) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{c.OpType, c.Key, c.Value})
}

func NewStore() *Store {
	return &Store{data: make(map[int]int)}
}

func (s *Store) Apply(txn []Op) ([]Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make([]Op, len(txn))
	for i, op := range txn {
		switch op.OpType {
		case "r":
			value := s.data[op.Key]
			res[i] = Op{"r", op.Key, &value}
		case "w":
			s.data[op.Key] = *op.Value
			res[i] = op
		default:
			return nil, fmt.Errorf("unknown op type: %s", op.OpType)
		}
	}
	return res, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	n := maelstrom.NewNode()
	store := NewStore()
	gossiper := NewGossiper(n)
	go gossiper.startGossip()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			Type string `json:"type"`
			Txn  []Op   `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res, err := store.Apply(body.Txn)
		if err != nil {
			return err
		}
		gossiper.Gossip(res)
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": res})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Type   string       `json:"type"`
			Gossip []GossipInfo `json:"gossip"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		txn := make([]Op, len(body.Gossip))
		for i, g := range body.Gossip {
			for _, w := range g.Writes {
				txn[i] = Op{"w", w[0], &w[1]}
			}
		}

		_, err := store.Apply(txn)
		if err != nil {
			return err
		}

		ids := make([]string, len(body.Gossip))
		for i, g := range body.Gossip {
			ids[i] = g.Id
		}
		return n.Reply(msg, map[string]any{"type": "gossip_ok", "gossip_ids": ids})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		var body struct {
			Type      string   `json:"type"`
			GossipIDs []string `json:"gossip_ids"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, id := range body.GossipIDs {
			gossiper.Ack(msg.Src, id)
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
