package hearthbot

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/gorilla/websocket"
)

type WSPacket struct {
	Command string `json:"command"`
	Data json.RawMessage `json:"data"`
}

type ActorInfo struct {
	IsBot bool  		`json:"is_bot"`
	IsAdmin bool  		`json:"is_admin"`
	Id int  			`json:"id"`
	DisplayName string 	`json:"display_name"`
}

type TopicInfo struct {
	SelfId int 					`json:"selfId"`
	SelfName string 			`json:"selfName"`
	Title string  				`json:"title"`
	Description string  		`json:"description"`
	PasswordProtected bool  	`json:"password_protected"`
	Authorized bool  			`json:"authorized"`
	CanWrite bool  				`json:"can_write"`
	RtpCapabilities any  		`json:"rtpCapabilities"`
	IceServers any  			`json:"iceServers"`
	Actors []ActorInfo 			`json:"actors"`
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

func (tc *TopicConnection) readLoop () {
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
		}
	}
}

func (tc *TopicConnection) writeLoop () {
	var closeErr error

	defer func() {
		tc.CloseWithError(closeErr)
	}()

	for {
		select {
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

func (tc *TopicConnection) pong () error {
	tc.Send("pong", json.RawMessage(`{}`))

	return nil
}

func (tc *TopicConnection) setup (data json.RawMessage) {
	var info TopicInfo

	if err := json.Unmarshal(data, &info); err != nil {
		tc.CloseWithError(err)
		return
	}

	tc.Info = info
	tc.initialized = true
}