package hearthbot

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type TopicConnection struct {
	conn        *websocket.Conn
	initialized bool
	password    string

	closed bool
	done   chan struct{}

	mu         sync.Mutex
	closeOnce  sync.Once
	send       chan WSPacket
	authorized chan bool

	chunkMutex   sync.Mutex
	pendingChunk chan MessageChunk

	Info   TopicInfo
	Actors []ActorInfo
	Hooks  TopicHooks
}

type TopicHooks struct {
	OnAuth              func(*TopicConnection)
	OnMessage           func(*TopicConnection, MessagePacket)
	OnClose             func(*TopicConnection, error)
	OnActorConnected    func(*TopicConnection, ActorInfo)
	OnActorDisconnected func(*TopicConnection, ActorInfo)
}

func (tc *TopicConnection) Close() error {
	return tc.CloseWithError(nil)
}

func (tc *TopicConnection) CloseWithError(closeErr error) error {
	var err error

	tc.closeOnce.Do(func() {
		tc.mu.Lock()
		tc.closed = true
		tc.initialized = false
		close(tc.done)
		close(tc.pendingChunk)
		conn := tc.conn
		tc.mu.Unlock()

		if conn != nil {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closed"),
				time.Now().Add(time.Second),
			)

			err = conn.Close()
		}

		if tc.Hooks.OnClose != nil {
			tc.Hooks.OnClose(tc, closeErr)
		}
	})

	return err
}

func (tc *TopicConnection) FetchMessages(offset int, direction rune, size uint) (MessageChunk, error) {
	tc.chunkMutex.Lock()
	defer tc.chunkMutex.Unlock()

	body, err := json.Marshal(ChunkRequest{
		Offset:    offset,
		Direction: direction,
		ChunkSize: size,
	})
	if err != nil {
		return MessageChunk{}, err
	}

	if err := tc.Send("fetchMessages", body); err != nil {
		return MessageChunk{}, err
	}

	select {
	case <-tc.done:
		return MessageChunk{}, fmt.Errorf("topic connection is closed")
	case result := <-tc.pendingChunk:
		return result, nil
	}
}

func (tc *TopicConnection) readLoop() {
	var closeErr error

	defer func() {
		tc.CloseWithError(closeErr)
	}()

	for {
		var packet WSPacket

		err := tc.conn.ReadJSON(&packet)
		if err != nil {
			closeErr = err
			return
		}

		switch packet.Command {
		case "ping":
			tc.pong()
		case "setup":
			tc.setup(packet.Data)
		case "authorize":
			tc.onAuth()
		case "actorConnected":
			tc.handleActorConnected(packet.Data)
		case "actorDisconnected":
			tc.handleActorDisconnected(packet.Data)
		case "chunkResponse":
			tc.handleChunkResponse(packet.Data)
		case "chunkError":
			tc.handleChunkError(packet.Data)
		case "message":
			tc.handleMessage(packet.Data)
		}
	}
}

func (tc *TopicConnection) writeLoop() {
	var closeErr error

	defer func() {
		tc.CloseWithError(closeErr)
	}()

	for {
		select {
		case auth := <-tc.authorized:
			if auth == false {
				closeErr = fmt.Errorf("Authorization failed")
				return
			}
		case packet := <-tc.send:
			err := tc.conn.WriteJSON(packet)
			if err != nil {
				closeErr = err
				return
			}

		case <-tc.done:
			return
		}
	}
}

func (tc *TopicConnection) pong() {
	tc.Send("pong", json.RawMessage(`{}`))
}

func (tc *TopicConnection) setup(data json.RawMessage) {
	var packet SetupPacket

	if err := json.Unmarshal(data, &packet); err != nil {
		tc.CloseWithError(err)
		return
	}

	tc.Info = packet.Info
	tc.Actors = packet.Actors
	tc.initialized = true

	if !tc.Info.Authorized {
		body, err := json.Marshal(MessagePush{
			Content:     tc.password,
			Attachments: make([]string, 0),
		})
		if err != nil {
			tc.CloseWithError(err)
			return
		}

		tc.Send("pushMessage", body)
	} else {
		tc.authorized <- true

		if tc.Hooks.OnAuth != nil {
			tc.Hooks.OnAuth(tc)
		}
	}
}

func (tc *TopicConnection) onAuth() {
	tc.Info.Authorized = true

	tc.authorized <- true

	if tc.Hooks.OnAuth != nil {
		tc.Hooks.OnAuth(tc)
	}
}

func (tc *TopicConnection) handleMessage(data json.RawMessage) {
	message := MessagePacket{}
	if err := json.Unmarshal(data, &message); err != nil {
		tc.CloseWithError(err)
		return
	}

	if tc.Hooks.OnMessage != nil {
		tc.Hooks.OnMessage(tc, message)
	}
}

func (tc *TopicConnection) handleActorConnected(data json.RawMessage) {
	info := ActorInfo{}
	if err := json.Unmarshal(data, &info); err != nil {
		tc.CloseWithError(err)
		return
	}

	tc.Actors = append(tc.Actors, info)

	if tc.Hooks.OnActorConnected != nil {
		tc.Hooks.OnActorConnected(tc, info)
	}
}

func (tc *TopicConnection) handleActorDisconnected(data json.RawMessage) {
	info := ActorInfo{}
	if err := json.Unmarshal(data, &info); err != nil {
		tc.CloseWithError(err)
		return
	}

	filtered := make([]ActorInfo, 0, len(tc.Actors))

	for _, actor := range tc.Actors {
		if actor.Id != info.Id {
			filtered = append(filtered, actor)
		}
	}

	tc.Actors = filtered

	if tc.Hooks.OnActorDisconnected != nil {
		tc.Hooks.OnActorDisconnected(tc, info)
	}
}

func (tc *TopicConnection) handleChunkResponse(data json.RawMessage) {
	chunk := ChunkData{}
	if err := json.Unmarshal(data, &chunk); err != nil {
		tc.CloseWithError(err)
		return
	}

	tc.pendingChunk <- MessageChunk{
		Error: false,
		Data:  chunk,
	}
}

func (tc *TopicConnection) handleChunkError(data json.RawMessage) {
	chunk := ChunkData{}
	if err := json.Unmarshal(data, &chunk); err != nil {
		tc.CloseWithError(err)
		return
	}

	tc.pendingChunk <- MessageChunk{
		Error: true,
		Data:  chunk,
	}
}

func (tc *TopicConnection) Send(command string, data json.RawMessage) error {
	tc.mu.Lock()
	closed := tc.closed
	tc.mu.Unlock()

	if closed {
		return fmt.Errorf("topic connection is closed")
	}

	packet := WSPacket{
		Command: command,
		Data:    data,
	}

	select {
	case tc.send <- packet:
		return nil

	case <-tc.done:
		return fmt.Errorf("topic connection is closed")
	}
}

func (tc *TopicConnection) SendText(text string) error {
	body, err := json.Marshal(MessagePush{
		Content:     text,
		Attachments: make([]string, 0),
	})
	if err != nil {
		return err
	}

	return tc.Send("pushMessage", body)
}

func (tc *TopicConnection) SendTextWithAttachment(text string, attachments []string) error {
	body, err := json.Marshal(MessagePush{
		Content:     text,
		Attachments: attachments,
	})
	if err != nil {
		return err
	}

	return tc.Send("pushMessage", body)
}
