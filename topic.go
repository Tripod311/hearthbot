package hearthbot

import (
	"sync"
	"fmt"
	"net/url"
	"net/http"
	"encoding/json"
	"github.com/gorilla/websocket"
)

type TopicMessage struct {
	Id int					`json:"id"`
	Content string			`json:"content"`
	Attachments []string	`json:"attachments"`
	ActorId int				`json:"actor_id"`
	DisplayName string		`json:"display_name"`
	CreatedAt int			`json:"created_at"`
	IsGuest bool 			`json:"is_guest"`
}

type TopicConnection struct {
	conn *websocket.Conn
	initialized bool
	Messages []TopicMessage
	StorageLimit uint
	password string

	closed bool
	done chan struct{}

	mu sync.Mutex
	closeOnce sync.Once
	send chan WSPacket

	Info TopicInfo
	Hooks TopicHooks
}

type TopicHooks struct {
	OnMessage func(*TopicConnection, *TopicMessage)
	OnClose func(*TopicConnection, error)
}

type wsRequest struct {
	TopicNode string `json:"topic_node"`
	TopicId int `json:"topic_id"`
}

type wsResponse struct {
	APIResponse
	Data string `json:"data"`
}

func (b *BotClient) ConnectTopic (id int, password string) (*TopicConnection, error) {
	requestBody := wsRequest{
		TopicNode: "self",
		TopicId: id,
	}

	data, err := json.Marshal(requestBody)
	if err != nil {
		b.emitError(err)
		return nil, err
	}

	responseBody, err := b.APIRequest(
		"/api/requestWS",
		http.MethodPost,
		data,
		"application/json",
	)
	if err != nil {
		b.emitError(err)
		return nil, err
	}

	var response wsResponse

	if err := json.Unmarshal(responseBody, &response); err != nil {
		b.emitError(err)
		return nil, err
	}

	if response.Error {
		err := fmt.Errorf("wsRequest failed: %s", response.Details)
		b.emitError(err)
		return nil, err
	}

	wsURL, err := buildWSURL(b.BaseURL, response.Data)
	if err != nil {
		b.emitError(err)
		return nil, err
	}

	u, err := url.Parse(b.BaseURL)
	if err != nil {
		b.emitError(err)
		return nil, err
	}

	headers := http.Header{}

	cookies := b.httpClient.Jar.Cookies(u)

	for _, cookie := range cookies {
		headers.Add("Cookie", cookie.String())
	}

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		if resp != nil {
			b.emitError(err)
			return nil, fmt.Errorf("websocket dial failed: %s: %w", resp.Status, err)
		} else {
			b.emitError(err)
			return nil, err
		}
	}

	topic := TopicConnection{
		conn: conn,
		initialized: false,
		Messages: make([]TopicMessage, 0),
		StorageLimit: 0,
		password: password,

		closed: false,
		done: make(chan struct{}),
		send: make(chan WSPacket, 64),

		Info: TopicInfo{},
		Hooks: TopicHooks{},
	}

	go topic.readLoop()
	go topic.writeLoop()

	return &topic, nil
}

func buildWSURL(baseURL string, key string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// do nothing
	default:
		return "", fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	u.Path = "/ws/" + key
	u.RawQuery = ""
	u.Fragment = ""

	return u.String(), nil
}