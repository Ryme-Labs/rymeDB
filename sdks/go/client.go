package rymedb

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Host    string
	Port    int
	Timeout time.Duration
}

func (c Client) roundTrip(parts []string) (string, error) {
	address := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	var frame strings.Builder
	fmt.Fprintf(&frame, "*%d\r\n", len(parts))
	for _, part := range parts {
		fmt.Fprintf(&frame, "$%d\r\n%s\r\n", len(part), part)
	}
	if _, err := conn.Write([]byte(frame.String())); err != nil {
		return "", err
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	header := strings.TrimSpace(line)
	if strings.HasPrefix(header, "$") {
		var length int
		fmt.Sscanf(header, "$%d", &length)
		if length < 0 {
			return "", nil
		}
		body := make([]byte, length+2)
		read := 0
		for read < len(body) {
			n, err := reader.Read(body[read:])
			if err != nil {
				return "", err
			}
			read += n
		}
		return string(body[:length]), nil
	}
	return header, nil
}

func (c Client) Get(key string) (string, bool, error) {
	value, err := c.roundTrip([]string{"GET", key})
	if err != nil {
		return "", false, err
	}
	if value == "" {
		return "", false, nil
	}
	return value, true, nil
}

func (c Client) Set(key string, value string) error {
	reply, err := c.roundTrip([]string{"SET", key, value})
	if err != nil {
		return err
	}
	if !strings.HasPrefix(reply, "+OK") {
		return fmt.Errorf("rymedb: %s", reply)
	}
	return nil
}
