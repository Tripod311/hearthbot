package hearthbot

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type TopicDescription struct {
	Id int `json:"id"`
	Title string `json:"title"`
	Description string `json:"description"`
	AuthorWriteOnly int `json:"author_write_only"`
	PasswordProtected int `json:"password_protected"`
	CreatorId int `json:"creator_id"`
	DisplayName string `json:"display_name"`
	GuestAccess int `json:"guest_access"`
}

type fetchTopicsResponse struct {
	APIResponse
	Data []TopicDescription `json:"data"`
}

type createTopicRequest struct {
	Title string `json:"title"`
	Description string `json:"description"`
	GuestAccess int `json:"guest_access"`
	AuthorWriteOnly int `json:"author_write_only"`
	Password string `json:"password,omitempty"`
}

type deleteTopicRequest struct {
	Id int `json:"id"`
}

func (b *BotClient) FetchTopics () error {
	responseBody, err := b.APIRequest(
		"/api/self/allTopics",
		http.MethodGet,
		nil,
		"",
	)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	var result fetchTopicsResponse

	if err := json.Unmarshal(responseBody, &result); err != nil {
		b.emitAuthError(err)
		return err
	}

	if result.Error {
		err := fmt.Errorf("allTopics failed: %s", result.Details)
		b.emitAuthError(err)
		return err
	}

	b.Topics = result.Data

	return nil
}

func (b *BotClient) CreateTopic (title string, description string, guestAccess int, authorWriteOnly int, password string) error {
	requestBody := createTopicRequest{
		Title: title,
		Description: description,
		GuestAccess: guestAccess,
		AuthorWriteOnly: authorWriteOnly,
		Password: password,
	}

	data, err := json.Marshal(requestBody)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	responseBody, err := b.APIRequest(
		"/api/createTopic",
		http.MethodPost,
		data,
		"application/json",
	)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	var result APIResponse

	if err := json.Unmarshal(responseBody, &result); err != nil {
		b.emitAuthError(err)
		return err
	}

	if result.Error {
		err := fmt.Errorf("createTopic failed: %s", result.Details)
		b.emitAuthError(err)
		return err
	}

	return nil
}

func (b *BotClient) DeleteTopic (id int) error {
	requestBody := deleteTopicRequest{
		Id: id,
	}

	data, err := json.Marshal(requestBody)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	responseBody, err := b.APIRequest(
		"/api/deleteTopic",
		http.MethodPost,
		data,
		"application/json",
	)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	var result APIResponse

	if err := json.Unmarshal(responseBody, &result); err != nil {
		b.emitAuthError(err)
		return err
	}

	if result.Error {
		err := fmt.Errorf("deleteTopic failed: %s", result.Details)
		b.emitAuthError(err)
		return err
	}

	return nil
}