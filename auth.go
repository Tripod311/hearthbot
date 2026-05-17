package hearthbot

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type authRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func (b *BotClient) Login() error {
	requestBody := authRequest{
		Login:    b.Username,
		Password: b.Password,
	}

	data, err := json.Marshal(requestBody)
	if err != nil {
		b.emitAuthError(err)
		return err
	}

	responseBody, err := b.APIRequest(
		"/api/login",
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
		err := fmt.Errorf("login failed: %s", result.Details)
		b.emitAuthError(err)
		return err
	}

	if b.Hooks.OnLoginSuccess != nil {
		b.Hooks.OnLoginSuccess()
	}

	return nil
}

func (b *BotClient) Logout() error {
	responseBody, err := b.APIRequest(
		"/api/logout",
		http.MethodPost,
		nil,
		"",
	)
	if err != nil {
		b.emitError(err)
		return err
	}

	var result APIResponse

	if err := json.Unmarshal(responseBody, &result); err != nil {
		b.emitError(err)
		return err
	}

	if result.Error {
		err := fmt.Errorf("logout failed: %s", result.Details)
		b.emitError(err)
		return err
	}

	return nil
}

func (b *BotClient) emitAuthError(err error) {
	if b.Hooks.OnLoginError != nil {
		b.Hooks.OnLoginError(err)
	}
}