package rymedb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/net/websocket"
)

type ChangeRecord struct {
	Tenant   string `json:"tenant"`
	Database string `json:"database"`
	Branch   string `json:"branch"`
	Table    string `json:"table"`
	Op       string `json:"op"`
	Pk       []int  `json:"pk"`
	After    []int  `json:"after"`
	CommitTs uint64 `json:"commit_ts"`
	Sequence uint64 `json:"sequence"`
}

type BroadcastMessage struct {
	Channel  string `json:"channel"`
	From     string `json:"from"`
	Payload  any    `json:"payload"`
	CommitTs uint64 `json:"commit_ts"`
	Sequence uint64 `json:"sequence"`
}

type QueryRow struct {
	Pk    string `json:"pk"`
	Value string `json:"value"`
}

type QueryMessage struct {
	Type      string     `json:"type"`
	Commit    uint64     `json:"commit"`
	Rows      []QueryRow `json:"rows"`
	Truncated bool       `json:"truncated"`
}

type Subscription struct {
	conn *websocket.Conn
}

func (s *Subscription) Next(v any) error {
	var text string
	if err := websocket.Message.Receive(s.conn, &text); err != nil {
		return err
	}
	return json.Unmarshal([]byte(text), v)
}

func (s *Subscription) Close() error {
	return s.conn.Close()
}

func (c *HttpClient) socketURL(path string, params map[string]string) string {
	base := strings.TrimRight(c.base, "/")
	if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else if strings.HasPrefix(base, "http://") {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	query := url.Values{}
	for key, value := range params {
		query.Set(key, value)
	}
	if c.apiKey != "" {
		query.Set("api_key", c.apiKey)
	}
	if encoded := query.Encode(); encoded != "" {
		return base + path + "?" + encoded
	}
	return base + path
}

func (c *HttpClient) SubscribeTable(table string) (*Subscription, error) {
	return c.SubscribeTableFrom(table, nil)
}

func (c *HttpClient) SubscribeTableFrom(table string, from *uint64) (*Subscription, error) {
	params := map[string]string{"table": table}
	if from != nil {
		params["from"] = fmt.Sprintf("%d", *from)
	}
	conn, err := websocket.Dial(
		c.socketURL("/v1/stream", params),
		"",
		fmt.Sprintf("%s/", strings.TrimRight(c.base, "/")),
	)
	if err != nil {
		return nil, err
	}
	return &Subscription{conn: conn}, nil
}

func (c *HttpClient) SubscribeBroadcast(channel string) (*Subscription, error) {
	conn, err := websocket.Dial(
		c.socketURL("/v1/broadcast/"+url.PathEscape(channel), map[string]string{}),
		"",
		fmt.Sprintf("%s/", strings.TrimRight(c.base, "/")),
	)
	if err != nil {
		return nil, err
	}
	return &Subscription{conn: conn}, nil
}

func (c *HttpClient) SubscribeQuery(table string, limit *int) (*Subscription, error) {
	params := map[string]string{"table": table}
	if limit != nil {
		params["limit"] = fmt.Sprintf("%d", *limit)
	}
	conn, err := websocket.Dial(
		c.socketURL("/v1/query-stream", params),
		"",
		fmt.Sprintf("%s/", strings.TrimRight(c.base, "/")),
	)
	if err != nil {
		return nil, err
	}
	return &Subscription{conn: conn}, nil
}
