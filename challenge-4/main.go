package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	counterKey = "counter"
)

type TimestampedValue struct {
	Count int `json:"count"`
	TS    int `json:"ts"`
}

type LinearKV struct {
	kv        *maelstrom.KV
	lastCount int
	lastTS    int
}

func NewLinearKV(kv *maelstrom.KV) *LinearKV {
	return &LinearKV{kv: kv}
}

func (lkv *LinearKV) Read() (int, error) {
	ctx := context.Background()
	curr := TimestampedValue{Count: lkv.lastCount, TS: lkv.lastTS}
	for {
		// force a read of the latest value by updating the timestamp
		next := TimestampedValue{Count: curr.Count, TS: curr.TS + 1}
		if err := lkv.kv.CompareAndSwap(ctx, counterKey, curr, next, true); err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
				if err := lkv.kv.ReadInto(ctx, counterKey, &curr); err != nil {
					return 0, err
				}
				lkv.lastCount, lkv.lastTS = curr.Count, curr.TS
				continue
			}
			return 0, fmt.Errorf("kv error: %w", err)
		}
		return curr.Count, nil
	}
}

func (lkv *LinearKV) Add(delta int) error {
	ctx := context.Background()
	curr := TimestampedValue{Count: lkv.lastCount, TS: lkv.lastTS}
	for {
		next := TimestampedValue{Count: curr.Count + delta, TS: curr.TS + 1}
		if err := lkv.kv.CompareAndSwap(ctx, counterKey, curr, next, true); err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
				if err := lkv.kv.ReadInto(ctx, counterKey, &curr); err != nil {
					return err
				}
				lkv.lastCount, lkv.lastTS = curr.Count, curr.TS
				continue
			}
			return fmt.Errorf("kv error: %w", err)
		}
		return nil
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	lkv := NewLinearKV(kv)

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val, err := lkv.Read()
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "value": val})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta, ok := body["delta"].(float64)
		if !ok {
			return fmt.Errorf("delta is not a number")
		}

		if err := lkv.Add(int(delta)); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
