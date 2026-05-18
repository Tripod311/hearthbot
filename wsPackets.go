package hearthbot

import (
	"encoding/json"
)

type WSPacket struct {
	Command string          `json:"command"`
	Data    json.RawMessage `json:"data"`
}

type ActorInfo struct {
	IsBot       int    `json:"is_bot"`
	IsAdmin     int    `json:"is_admin"`
	Id          int    `json:"id"`
	DisplayName string `json:"display_name"`
}

type TopicInfo struct {
	SelfId            int    `json:"selfId"`
	Title             string `json:"title"`
	Description       string `json:"description"`
	PasswordProtected int    `json:"password_protected"`
	Authorized        bool   `json:"authorized"`
	CanWrite          bool   `json:"can_write"`
}

type SetupPacket struct {
	RtpCapabilities json.RawMessage `json:"rtpCapabilities"`
	IceServers      json.RawMessage `json:"iceServers"`
	Info            TopicInfo       `json:"topicInfo"`
	Actors          []ActorInfo     `json:"actors"`
}

type MessagePacket struct {
	Id          int      `json:"id"`
	ActorId     int      `json:"actor_id"`
	DisplayName string   `json:"display_name"`
	Content     string   `json:"content"`
	Attachments []string `json:"attachments"`
	CreatedAt   int      `json:"created_at"`
}

type MessagePush struct {
	Content     string   `json:"content"`
	Attachments []string `json:"attachments"`
}

type ChunkRequest struct {
	Offset    int  `json:"offset"`
	Direction rune `json:"direction"`
	ChunkSize uint `json:"chunk_size"`
}

type ChunkData struct {
	RequestedOffset int             `json:"requestedOffset"`
	Messages        []MessagePacket `json:"messages"`
}

type MessageChunk struct {
	Error bool
	Data  ChunkData
}
