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
	dest    string
	ackID   int
	message map[string]any
	acktype string
}

type MessageSender struct {
	n            *maelstrom.Node
	nextAckID    int
	outbox       chan outmsg
	awaitingAcks []int
}

func NewMessageSender(n *maelstrom.Node) *MessageSender {
	outbox := make(chan outmsg)
	awaitingAcks := make([]int, 0)
	nextAckID := 1
	return &MessageSender{n, nextAckID, outbox, awaitingAcks}
}

func (m *MessageSender) Start() {
	go m.sendMessages()
}

func (m *MessageSender) SendWithRetries(dest string, message map[string]any, acktype string) {
	ackID := m.nextAckID
	message["ack_id"] = ackID
	m.awaitingAcks = append(m.awaitingAcks, ackID)
	m.outbox <- outmsg{dest, ackID, message, acktype}
	m.nextAckID++
}

func (m *MessageSender) Ack(typ string, ackID int) {
	// remove ackID from awaitingAcks
	_ = slices.DeleteFunc(m.awaitingAcks, func(ackID2 int) bool {
		return ackID2 == ackID
	})
}

func (m *MessageSender) sendMessages() {
	for o := range m.outbox {
		if !slices.Contains(m.awaitingAcks, o.ackID) {
			// this message has already been acked, do not re-send
			continue
		}
		err := m.n.Send(o.dest, o.message)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		go func() {
			time.Sleep(1 * time.Second)
			m.outbox <- o
		}()
	}
}

func main() {
	n := maelstrom.NewNode()

	mu := sync.Mutex{}  // guards `messages`
	messages := []int{} // all messages received to date

	m := NewMessageSender(n)
	m.Start()

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
				message := map[string]any{"type": "gossip", "message": val}
				m.SendWithRetries(n2, message, "gossip_ok")
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

		m.Ack("gossip_ok", int(ackID))
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
