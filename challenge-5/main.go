package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Store struct {
	n    *maelstrom.Node
	kv   *maelstrom.KV
	mu   sync.Mutex
	data map[string][]int // only this node's data is stored here
}

func NewStore(n *maelstrom.Node, kv *maelstrom.KV) *Store {
	return &Store{n, kv, sync.Mutex{}, make(map[string][]int)}
}

// AddMessage appends a message to the log for a given key and returns the offset of the message.
func (s *Store) AddMessage(ctx context.Context, key string, msg int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kvkey := fmt.Sprintf("log/%s", key)
	currData, ok := s.data[key]
	if !ok {
		currData = make([]int, 0)
	}

	newData := append(currData, msg)
	s.data[key] = newData
	if err := s.kv.Write(ctx, kvkey, newData); err != nil {
		return 0, fmt.Errorf("kv error: %w", err)
	}
	return len(newData) - 1, nil
}

// Poll returns the messages for a given set of offsets.
// Returns a map from keys to arrays of [offset, message] pairs.
func (s *Store) Poll(ctx context.Context, offsets map[string]int) (map[string][][2]int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	res := make(map[string][][2]int)
	errCh := make(chan error, len(offsets))

	for key, offset := range offsets {
		wg.Add(1)
		go func(key string, offset int) {
			defer wg.Done()
			data, err := s.getLog(ctx, key)
			if err != nil {
				errCh <- err
				return
			}

			messages := make([][2]int, 0, len(data[offset:]))
			for idx, msg := range data[offset:] {
				messages = append(messages, [2]int{offset + idx, msg})
			}

			mu.Lock()
			res[key] = messages
			mu.Unlock()
		}(key, offset)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err // return first error encountered
	}

	return res, nil
}

// getLog returns all of the messages of a log with the given key.
func (s *Store) getLog(ctx context.Context, key string) ([]int, error) {
	// if the key is on this node, return the data from the local store
	keyInt := keyToInt(key)
	targetNode := s.n.NodeIDs()[keyInt%len(s.n.NodeIDs())]
	if targetNode == s.n.ID() {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.data[key], nil
	}

	kvkey := fmt.Sprintf("log/%s", key)
	var data []int
	if err := s.kv.ReadInto(ctx, kvkey, &data); err != nil {
		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return make([]int, 0), nil
		}
		return nil, fmt.Errorf("kv error: %w", err)
	}
	return data, nil
}

// SetCommitOffsets sets the commit offsets for a given set of keys.
func (s *Store) SetCommitOffsets(ctx context.Context, offsets map[string]int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(offsets))

	for key, offset := range offsets {
		wg.Add(1)
		go func(key string, offset int) {
			defer wg.Done()
			if err := s.SetCommitOffset(ctx, key, offset); err != nil {
				errCh <- err
			}
		}(key, offset)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return err // return first error encountered
	}

	return nil
}

// SetCommitOffset sets the commit offset for a given key.
// Setting the offset for a key to a lower value than the current offset is a no-op.
func (s *Store) SetCommitOffset(ctx context.Context, key string, offset int) error {
	kvkey := fmt.Sprintf("offset/%s", key)
	for {
		// get the current offset
		currOffset, err := s.kv.ReadInt(ctx, kvkey)
		if err != nil {
			rpcErr, ok := err.(*maelstrom.RPCError)
			if !ok || rpcErr.Code != maelstrom.KeyDoesNotExist {
				return fmt.Errorf("kv error: %w", err)
			}
			currOffset = 0
		}

		// return if we've already processed messages up to this offset
		if offset <= currOffset {
			return nil
		}

		// compare-and-swap to update the KV store
		if err := s.kv.CompareAndSwap(ctx, kvkey, currOffset, offset, true); err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
				// if the CAS fails, retry
				continue
			}
			return fmt.Errorf("kv error: %w", err)
		}
		return nil
	}
}

// GetCommitOffsets returns the commit offsets for a given set of keys.
func (s *Store) GetCommitOffsets(ctx context.Context, keys []string) (map[string]int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	res := make(map[string]int)
	errCh := make(chan error, len(keys))

	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			offset, err := s.GetCommitOffset(ctx, key)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			res[key] = offset
			mu.Unlock()
		}(key)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err // return the first error encountered
	}

	return res, nil
}

// GetCommitOffset returns the commit offset for a given key.
func (s *Store) GetCommitOffset(ctx context.Context, key string) (int, error) {
	offset, err := s.kv.ReadInt(ctx, fmt.Sprintf("offset/%s", key))
	if err != nil {
		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
			return 0, nil
		}
		return 0, fmt.Errorf("kv error: %w", err)
	}
	return offset, err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewStore(n, kv)

	n.Handle("send", func(msg maelstrom.Message) error {
		ctx := context.Background()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key, ok := body["key"].(string)
		if !ok {
			return errors.New("key is not a string")
		}
		mesg, ok := body["msg"].(float64)
		if !ok {
			return errors.New("msg is not a float64")
		}

		keyInt := keyToInt(key)

		targetNode := n.NodeIDs()[keyInt%len(n.NodeIDs())]
		if targetNode != n.ID() {
			// forward the message to another node
			msg2, err := n.SyncRPC(ctx, targetNode, msg.Body)
			if err != nil {
				return fmt.Errorf("failed to forward message: %w", err)
			}

			var body2 map[string]any
			if err := json.Unmarshal(msg2.Body, &body2); err != nil {
				return fmt.Errorf("failed to unmarshal message: %w", err)
			}

			if body2["type"] != "send_ok" {
				return fmt.Errorf("unexpected message type: %s", body2["type"])
			}

			return n.Reply(msg, map[string]any{"type": "send_ok", "offset": body2["offset"]})
		}

		offset, err := s.AddMessage(ctx, key, int(mesg))
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("send_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets, ok := body["offsets"].(map[string]any)
		if !ok {
			return errors.New("offsets is not a map")
		}
		offsets2 := make(map[string]int)
		for key, offset := range offsets {
			offsets2[key] = int(offset.(float64))
		}

		messages, err := s.Poll(context.Background(), offsets2)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": messages})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets, ok := body["offsets"].(map[string]any)
		if !ok {
			return errors.New("offsets is not a map")
		}
		offsets2 := make(map[string]int)
		for key, offset := range offsets {
			offsets2[key] = int(offset.(float64))
		}

		if err := s.SetCommitOffsets(context.Background(), offsets2); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys, ok := body["keys"].([]any)
		if !ok {
			return errors.New("keys is not a slice")
		}
		keys2 := make([]string, 0)
		for _, key := range keys {
			keys2 = append(keys2, key.(string))
		}

		offsets, err := s.GetCommitOffsets(context.Background(), keys2)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func keyToInt(key string) int {
	keyInt, err := strconv.Atoi(key)
	if err != nil {
		panic(fmt.Sprintf("key is not an integer: %s", key))
	}
	return keyInt
}
