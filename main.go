package main

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed frontend
var embeddedFrontend embed.FS

// ── Constants ─────────────────────────────────────────────────────────────────

const (
	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second
	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
	// Maximum message size allowed from peer.
	maxMessageSize = 4096
)

// ── Types ────────────────────────────────────────────────────────────────────

type Message struct {
	Type    string          `json:"type"`
	Room    string          `json:"room,omitempty"`
	Role    string          `json:"role,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Peer struct {
	conn *websocket.Conn
	send chan Message
}

type Room struct {
	sender   *Peer
	receiver *Peer
	created  time.Time
	// paired is true once both sender and receiver have joined; paired rooms
	// are never reaped by the stale-room GC.
	paired bool
}

// ── Hub ──────────────────────────────────────────────────────────────────────

type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*Room
}

func newHub() *Hub {
	h := &Hub{rooms: make(map[string]*Room)}
	go h.reapStaleRooms()
	return h
}

// reapStaleRooms removes rooms that have been waiting for a peer pair for more
// than 10 minutes.  Fully-paired rooms are never reaped here — they clean up
// themselves when a peer disconnects.
func (h *Hub) reapStaleRooms() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		h.mu.Lock()
		for id, room := range h.rooms {
			// Only reap unpaired rooms that have been waiting too long.
			if room.paired {
				continue
			}
			if now.Sub(room.created) <= 10*time.Minute {
				continue
			}
			log.Printf("[hub] reaping stale room %s", id)

			// Nil the pointers inside the lock before closing the channels so
			// that concurrent sendMsg calls see a nil peer and drop the message
			// instead of sending to a closed channel.
			sender := room.sender
			receiver := room.receiver
			room.sender = nil
			room.receiver = nil
			delete(h.rooms, id)

			// Channel closes happen outside-ish but still under the lock is
			// fine; writePump only reads from the channel, it never sends.
			if sender != nil {
				close(sender.send)
			}
			if receiver != nil {
				close(receiver.send)
			}
		}
		h.mu.Unlock()
	}
}

// ── WebSocket upgrader ───────────────────────────────────────────────────────

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ── Peer pump ────────────────────────────────────────────────────────────────

func newPeer(conn *websocket.Conn) *Peer {
	return &Peer{conn: conn, send: make(chan Message, 32)}
}

func (p *Peer) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		p.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-p.send:
			p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// Channel was closed; send a close frame and exit.
				p.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := p.conn.WriteJSON(msg); err != nil {
				log.Printf("[peer] write error: %v", err)
				return
			}
		case <-ticker.C:
			p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (p *Peer) sendMsg(msg Message) {
	select {
	case p.send <- msg:
	default:
		log.Printf("[peer] send buffer full, dropping message type=%s", msg.Type)
	}
}

func errMsg(reason string) Message {
	payload, _ := json.Marshal(reason)
	return Message{Type: "error", Payload: json.RawMessage(payload)}
}

// ── Handler ──────────────────────────────────────────────────────────────────

func (h *Hub) wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	// Configure read limits and initial deadline.
	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	peer := newPeer(conn)
	go peer.writePump()

	var roomID string
	var role string

	defer func() {
		// cleanup: remove peer from room and notify the other side
		if roomID == "" {
			conn.Close()
			return
		}
		h.mu.Lock()
		room, ok := h.rooms[roomID]
		if ok {
			var other *Peer
			if role == "sender" {
				room.sender = nil
				other = room.receiver
			} else {
				room.receiver = nil
				other = room.sender
			}
			// if both gone, delete room
			if room.sender == nil && room.receiver == nil {
				delete(h.rooms, roomID)
				log.Printf("[hub] room %s deleted (both peers gone)", roomID)
			}
			h.mu.Unlock()

			if other != nil {
				other.sendMsg(Message{Type: "peer_left"})
				close(other.send)
			}
		} else {
			h.mu.Unlock()
		}
		conn.Close()
	}()

	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error: %v", err)
			}
			break
		}

		switch msg.Type {

		case "join":
			if msg.Room == "" || (msg.Role != "sender" && msg.Role != "receiver") {
				peer.sendMsg(errMsg("invalid_join"))
				continue
			}
			roomID = msg.Room
			role = msg.Role

			h.mu.Lock()
			room, exists := h.rooms[roomID]

			if role == "sender" {
				if exists && room.sender != nil {
					h.mu.Unlock()
					peer.sendMsg(errMsg("room_taken"))
					roomID = ""
					role = ""
					continue
				}
				if !exists {
					room = &Room{created: time.Now()}
					h.rooms[roomID] = room
				}
				room.sender = peer
				h.mu.Unlock()
				log.Printf("[hub] sender joined room %s", roomID)
				peer.sendMsg(Message{Type: "joined", Room: roomID})

			} else { // receiver
				if !exists || room.sender == nil {
					h.mu.Unlock()
					peer.sendMsg(errMsg("room_not_found"))
					roomID = ""
					role = ""
					continue
				}
				if room.receiver != nil {
					h.mu.Unlock()
					peer.sendMsg(errMsg("room_full"))
					roomID = ""
					role = ""
					continue
				}
				room.receiver = peer
				room.paired = true
				sender := room.sender
				h.mu.Unlock()
				log.Printf("[hub] receiver joined room %s", roomID)
				peer.sendMsg(Message{Type: "joined", Room: roomID})
				// notify sender that receiver is ready
				sender.sendMsg(Message{Type: "ready"})
			}

		case "offer", "answer", "ice":
			if roomID == "" {
				peer.sendMsg(errMsg("not_in_room"))
				continue
			}
			h.mu.RLock()
			room, ok := h.rooms[roomID]
			h.mu.RUnlock()
			if !ok {
				peer.sendMsg(errMsg("room_not_found"))
				continue
			}

			// relay to the other peer
			var target *Peer
			if role == "sender" {
				target = room.receiver
			} else {
				target = room.sender
			}
			if target == nil {
				peer.sendMsg(errMsg("peer_not_connected"))
				continue
			}
			target.sendMsg(Message{Type: msg.Type, Payload: msg.Payload})

		default:
			log.Printf("[ws] unknown message type: %s", msg.Type)
		}
	}
}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	hub := newHub()

	// Serve embedded frontend static files.
	sub, err := fs.Sub(embeddedFrontend, "frontend")
	if err != nil {
		log.Fatalf("[p2nano] embed error: %v", err)
	}
	http.Handle("/", http.FileServer(http.FS(sub)))

	// WebSocket signaling endpoint
	http.HandleFunc("/ws", hub.wsHandler)

	log.Printf("[p2nano] listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[p2nano] fatal: %v", err)
	}
}
