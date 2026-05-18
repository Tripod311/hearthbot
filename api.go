package hearthbot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

type APIResponse struct {
	Error   bool   `json:"error"`
	Details string `json:"details,omitempty"`
}

type UploadFile struct {
	Name    string
	Content []byte
}

type uploadFilesResponse struct {
	Error   bool     `json:"error"`
	Details string   `json:"details"`
	Data    []string `json:"data"`
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

func (b *BotClient) GetFile(fileName string, w io.Writer) error {
	url := b.BaseURL + "/self/files/" + fileName

	resp, err := b.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	_, err = io.Copy(w, resp.Body)
	return err
}

func (b *BotClient) UploadFiles(files []UploadFile) ([]string, error) {
	return b.UploadFilesStream(func(writer *multipart.Writer) error {
		for i, data := range files {
			part, err := writer.CreateFormFile(fmt.Sprintf("%d", i), data.Name)
			if err != nil {
				return err
			}

			_, err = io.Copy(part, bytes.NewReader(data.Content))
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *BotClient) UploadFilesFromDisk(paths []string) ([]string, error) {
	return b.UploadFilesStream(func(writer *multipart.Writer) error {
		for i, path := range paths {
			file, err := os.Open(path)
			if err != nil {
				return err
			}

			fileName := filepath.Base(path)

			part, err := writer.CreateFormFile(fmt.Sprintf("%d", i), fileName)
			if err != nil {
				file.Close()
				return err
			}

			_, err = io.Copy(part, file)
			closeErr := file.Close()

			if err != nil {
				return err
			}

			if closeErr != nil {
				return closeErr
			}
		}

		return nil
	})
}

func (b *BotClient) UploadFilesStream(writeMultipart func(writer *multipart.Writer) error) ([]string, error) {
	url := b.BaseURL + "/api/uploadFiles"

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	req, err := http.NewRequest("POST", url, pr)
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	go func() {
		err := writeMultipart(writer)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		err = writer.Close()
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		_ = pw.Close()
	}()

	resp, err := b.httpClient.Do(req)
	if err != nil {
		_ = pr.Close()
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload files failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var result uploadFilesResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	if result.Error {
		return nil, fmt.Errorf("upload files failed: %s", result.Details)
	}

	return result.Data, nil
}
