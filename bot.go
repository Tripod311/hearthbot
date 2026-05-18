package hearthbot

import (
	"net/http"
	"net/http/cookiejar"
	"time"
)

type BotClient struct {
	BaseURL  string
	Username string
	Password string

	httpClient *http.Client
	transport  *http.Transport
	jar        http.CookieJar

	token string

	Topics []TopicDescription

	Hooks BotHooks
}

type BotHooks struct {
	OnLoginSuccess func()
	OnLoginError   func(error)
	OnError        func(error)
}

func NewBotClient(baseURL, username, password string) *BotClient {
	jar, err := cookiejar.New(nil)
	if err != nil {
		panic(err)
	}

	transport := &http.Transport{}

	return &BotClient{
		BaseURL:  baseURL,
		Username: username,
		Password: password,

		transport: transport,
		jar:       jar,
		httpClient: &http.Client{
			Timeout:   15 * time.Second,
			Transport: transport,
			Jar:       jar,
		},
	}
}

func (b *BotClient) Close() {
	if b.transport != nil {
		b.transport.CloseIdleConnections()
	}
}

func (b *BotClient) emitError(err error) {
	if b.Hooks.OnError != nil {
		b.Hooks.OnError(err)
	}
}
