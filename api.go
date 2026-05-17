package hearthbot

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type APIResponse struct {
	Error bool `json:"error"`
	Details string `json:"details,omitempty"`
}

func (b *BotClient) APIRequest(path string, method string, body []byte, contentType string) ([]byte, error) {
	url := b.BaseURL + path

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	res, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return resBody, fmt.Errorf("api request failed: %s", res.Status)
	}

	return resBody, nil
}