package downloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type ariaClient struct {
	url    string
	secret string
	http   *http.Client
	nextID atomic.Uint64
}

type ariaRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *ariaRPCError) Error() string {
	return fmt.Sprintf("aria2 RPC %d: %s", e.Code, e.Message)
}

type ariaStatus struct {
	GID             string `json:"gid"`
	Status          string `json:"status"`
	TotalLength     string `json:"totalLength"`
	CompletedLength string `json:"completedLength"`
	DownloadSpeed   string `json:"downloadSpeed"`
	ErrorCode       string `json:"errorCode"`
	ErrorMessage    string `json:"errorMessage"`
	Files           []struct {
		Path string `json:"path"`
	} `json:"files"`
}

func newAriaClient(port int, secret string) *ariaClient {
	return &ariaClient{
		url:    fmt.Sprintf("http://127.0.0.1:%d/jsonrpc", port),
		secret: secret,
		http:   &http.Client{Timeout: 8 * time.Second},
	}
}

func (c *ariaClient) call(method string, params []any, result any) error {
	requestParams := make([]any, 0, len(params)+1)
	requestParams = append(requestParams, "token:"+c.secret)
	requestParams = append(requestParams, params...)
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID.Add(1),
		"method":  method,
		"params":  requestParams,
	})
	if err != nil {
		return err
	}
	resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *ariaRPCError   `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("decode aria2 RPC response: %w", err)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode aria2 RPC result: %w", err)
	}
	return nil
}

func (c *ariaClient) ready() error {
	var version struct {
		Version string `json:"version"`
	}
	return c.call("aria2.getVersion", nil, &version)
}

func (c *ariaClient) addURI(t *Task, options map[string]any) error {
	params := []any{[]string{t.Link}, options}
	if t.QueueID > 0 {
		params = append(params, t.QueueID)
	}
	var gid string
	if err := c.call("aria2.addUri", params, &gid); err != nil {
		return err
	}
	if gid != t.GID {
		return fmt.Errorf("aria2 returned unexpected GID %q (wanted %q)", gid, t.GID)
	}
	return nil
}

func (c *ariaClient) pause(gid string) error {
	var result string
	return c.call("aria2.pause", []any{gid}, &result)
}

func (c *ariaClient) unpause(gid string) error {
	var result string
	return c.call("aria2.unpause", []any{gid}, &result)
}

func (c *ariaClient) removeResult(gid string) error {
	var result string
	return c.call("aria2.removeDownloadResult", []any{gid}, &result)
}

func (c *ariaClient) shutdown() error {
	var result string
	return c.call("aria2.shutdown", nil, &result)
}

func (c *ariaClient) statuses() ([]ariaStatus, error) {
	keys := []string{"gid", "status", "totalLength", "completedLength", "downloadSpeed", "errorCode", "errorMessage", "files"}
	var active, waiting, stopped []ariaStatus
	if err := c.call("aria2.tellActive", []any{keys}, &active); err != nil {
		return nil, err
	}
	if err := c.call("aria2.tellWaiting", []any{0, 10000, keys}, &waiting); err != nil {
		return nil, err
	}
	if err := c.call("aria2.tellStopped", []any{0, 256, keys}, &stopped); err != nil {
		return nil, err
	}
	result := make([]ariaStatus, 0, len(active)+len(waiting)+len(stopped))
	result = append(result, active...)
	result = append(result, waiting...)
	result = append(result, stopped...)
	return result, nil
}

func isGIDNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "gid") && strings.Contains(strings.ToLower(err.Error()), "not found")
}
