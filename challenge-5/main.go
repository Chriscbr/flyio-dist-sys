package main

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Store struct {
	mu      sync.Mutex
	offsets map[string]int
	logs    map[string][]int
}

func NewStore() *Store {
	return &Store{
		offsets: make(map[string]int),
		logs:    make(map[string][]int),
	}
}

func (s *Store) AddMessage(key string, msg int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log, ok := s.logs[key]
	if !ok {
		s.logs[key] = make([]int, 0)
		log = s.logs[key]
	}

	log = append(log, msg)
	s.logs[key] = log
	return len(log) - 1, nil
}

func (s *Store) Poll(offsets map[string]int) (map[string][][]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string][][]int)
	for key, offset := range offsets {
		log, ok := s.logs[key]
		if !ok {
			log = make([]int, 0)
			s.logs[key] = log
		}
		res[key] = make([][]int, 0)
		for idx, msg := range log[offset:] {
			res[key] = append(res[key], []int{offset + idx, msg})
		}
	}
	return res, nil
}

func (s *Store) SetCommitOffsets(offsets map[string]int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, offset := range offsets {
		s.offsets[key] = offset
	}
	return nil
}

func (s *Store) GetCommitOffsets(offsets []string) (map[string]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]int)
	for _, key := range offsets {
		offset, ok := s.offsets[key]
		if !ok {
			offset = 0
			s.offsets[key] = offset
		}
		res[key] = offset
	}
	return res, nil
}

func main() {
	n := maelstrom.NewNode()
	s := NewStore()

	n.Handle("send", func(msg maelstrom.Message) error {
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

		offset, err := s.AddMessage(key, int(mesg))
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
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

		messages, err := s.Poll(offsets2)
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

		if err := s.SetCommitOffsets(offsets2); err != nil {
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

		offsets, err := s.GetCommitOffsets(keys2)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
