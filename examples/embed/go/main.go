package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func encode(parts ...any) []byte {
	var out bytes.Buffer
	fmt.Fprintf(&out, "*%d\r\n", len(parts))
	for _, part := range parts {
		var body []byte
		switch value := part.(type) {
		case []byte:
			body = value
		case string:
			body = []byte(value)
		default:
			body = []byte(fmt.Sprint(value))
		}
		fmt.Fprintf(&out, "$%d\r\n", len(body))
		out.Write(body)
		out.WriteString("\r\n")
	}
	return out.Bytes()
}

func readValue(r *bufio.Reader) (any, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch kind {
	case '+':
		return line, nil
	case '-':
		return nil, fmt.Errorf("%s", line)
	case ':':
		return strconv.ParseInt(line, 10, 64)
	case '$':
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}
		body := make([]byte, length+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		return body[:length], nil
	case '*':
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		items := make([]any, count)
		for i := range items {
			items[i], err = readValue(r)
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported RESP byte %q", kind)
	}
}

func start(bin, db string) (*exec.Cmd, string, error) {
	cmd := exec.Command(bin, "serve", "--db", db, "--no-amqp", "--native", "127.0.0.1:0")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	if err := cmd.Start(); err != nil {
		return nil, "", err
	}

	lines := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "native ") {
				lines <- strings.Fields(scanner.Text())[1]
				return
			}
		}
	}()

	select {
	case address := <-lines:
		return cmd, address, nil
	case <-time.After(10 * time.Second):
		return cmd, "", fmt.Errorf("startup timed out")
	}
}

func main() {
	bin := flag.String("bin", envOr("ANTE_BIN", "ante"), "path to the ante binary")
	flag.Parse()

	dir, err := os.MkdirTemp("", "ante-embed-go-")
	check(err)
	defer os.RemoveAll(dir)

	cmd, address, err := start(*bin, dir+"/ante.sqlite3")
	check(err)
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()

	conn, err := net.Dial("tcp", address)
	check(err)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	request := func(parts ...any) any {
		_, err := conn.Write(encode(parts...))
		check(err)
		value, err := readValue(reader)
		check(err)
		return value
	}

	fmt.Println("hello", request("HELLO", "1"))
	fmt.Println("declare", request("DECLARE", "demo"))

	payload, _ := json.Marshal(map[string]any{"lang": "go", "n": 1})
	fmt.Println("publish", request("PUBLISH", "demo", payload, "application/json"))

	record := request("TAKE", "demo", "1", "30000", "5000").([]any)[0].([]any)
	token := string(record[0].([]byte))
	var decoded map[string]any
	check(json.Unmarshal(record[6].([]byte), &decoded))
	fmt.Println("took", map[string]any{
		"token":     token,
		"messageId": string(record[1].([]byte)),
		"attempts":  string(record[5].([]byte)),
		"payload":   decoded,
	})

	fmt.Println("ack", request("ACK", token))
	info := request("INFO", "demo").([]any)
	fields := make([]string, len(info))
	for i, value := range info {
		fields[i] = string(value.([]byte))
	}
	fmt.Println("info", strings.Join(fields, " "))
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}