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

func NewGossipInfo(writes [][]int) GossipInfo {
	return GossipInfo{
		Id:     uuid.NewString(),
		Writes: writes,
	}
}

func NewGossiper(n *maelstrom.Node) *Gossiper {
	pending := make(map[string][]GossipInfo)
	return &Gossiper{n, sync.Mutex{}, pending}
}

func (g *Gossiper) Gossip(txn []Op) {
	g.mu.Lock()
	defer g.mu.Unlock()

	writes := make([][]int, 0)
	for _, op := range txn {
		if op.OpType == "w" {
			writes = append(writes, []int{op.Key, *op.Value})
		}
	}

	// don't gossip if there are no writes
	if len(writes) == 0 {
		return
	}

	for _, n := range g.n.NodeIDs() {
		if n == g.n.ID() { // don't gossip to yourself
			continue
		}
		g.pending[n] = append(g.pending[n], NewGossipInfo(writes))
	}
}

func (g *Gossiper) startGossip() {
	ticker := time.NewTicker(gossipBatchRate)
	for range ticker.C {
		g.mu.Lock()
		for _, n := range g.n.NodeIDs() {
			if n == g.n.ID() { // don't gossip to yourself
				continue
			}
			if len(g.pending[n]) > 0 {
				body := make(map[string]any)
				body["type"] = "gossip"
				body["gossip"] = g.pending[n]
				err := g.n.Send(n, body)
				if err != nil {
					log.Printf("Error: %v", err)
				}
			}
		}
		g.mu.Unlock()
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
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return errors.New("expected array of length 3")
	}

	var opType string
	var key int
	var value *int
	if err := json.Unmarshal(raw[0], &opType); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &key); err != nil {
		return err
	}
	if string(raw[2]) != "null" { // check for null before unmarshaling
		var v int
		if err := json.Unmarshal(raw[2], &v); err != nil {
			return err
		}
		value = &v
	}

	op.OpType = opType
	op.Key = key
	op.Value = value
	return nil
}

func (c Op) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{c.OpType, c.Key, c.Value})
}

func NewStore() *Store {
	return &Store{
		data: make(map[int]int),
	}
}

func (s *Store) Apply(txn []Op) ([]Op, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make([]Op, 0, len(txn))
	for _, op := range txn {
		switch op.OpType {
		case "r":
			value := s.data[op.Key]
			res = append(res, Op{op.OpType, op.Key, &value})
		case "w":
			s.data[op.Key] = *op.Value
			res = append(res, op)
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
		type TxnBody struct {
			Type string `json:"type"`
			Txn  []Op   `json:"txn"`
		}

		var body TxnBody
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
		type GossipBody struct {
			Type   string       `json:"type"`
			Gossip []GossipInfo `json:"gossip"`
		}

		var body GossipBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		txn := make([]Op, 0, len(body.Gossip))
		for _, gossipInfo := range body.Gossip {
			for _, write := range gossipInfo.Writes {
				txn = append(txn, Op{"w", write[0], &write[1]})
			}
		}

		_, err := store.Apply(txn)
		if err != nil {
			return err
		}

		ids := make([]string, 0, len(body.Gossip))
		for _, gossipInfo := range body.Gossip {
			ids = append(ids, gossipInfo.Id)
		}

		return n.Reply(msg, map[string]any{"type": "gossip_ok", "gossip_ids": ids})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		type GossipOkBody struct {
			Type      string   `json:"type"`
			GossipIDs []string `json:"gossip_ids"`
		}

		var body GossipOkBody
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
