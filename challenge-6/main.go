package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Store struct {
	mu   sync.Mutex
	data map[int]int
}

type Op = []any

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
		opType := op[0].(string)
		key := int(op[1].(float64))
		var value int
		if op[2] != nil {
			value = int(op[2].(float64))
		}
		switch opType {
		case "r":
			res = append(res, Op{opType, key, s.data[key]})
		case "w":
			s.data[key] = int(value)
			res = append(res, Op{opType, key, value})
		default:
			return nil, fmt.Errorf("unknown op type: %s", opType)
		}
	}
	return res, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	n := maelstrom.NewNode()
	store := NewStore()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		rawTxn, ok := body["txn"].([]any)
		if !ok {
			return errors.New("txn is not an array")
		}

		txn := make([]Op, 0, len(rawTxn))
		for _, op := range rawTxn {
			txn = append(txn, op.(Op))
		}

		res, err := store.Apply(txn)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": res})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
