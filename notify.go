package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type wsMessage struct {
	Message struct {
		Attributes map[string]string `json:"attributes"`
	} `json:"message"`
}

type Notifier struct {
	account    *Account
	onChange   func()
	stopCh     chan struct{}
	pollSecs   int
}

func NewNotifier(acct *Account, pollSecs int, onChange func()) *Notifier {
	return &Notifier{
		account:  acct,
		onChange: onChange,
		stopCh:   make(chan struct{}),
		pollSecs: pollSecs,
	}
}

func (n *Notifier) Stop() {
	close(n.stopCh)
}

func (n *Notifier) Run() {
	wsURL := n.discoverWSURL()
	if wsURL != "" {
		log.Printf("[%s] attempting WebSocket connection to %s", n.account.Name, wsURL)
		n.runWebSocket(wsURL)
		return
	}
	log.Printf("[%s] WebSocket not available, using polling (every %ds)", n.account.Name, n.pollSecs)
	n.runPolling()
}

func (n *Notifier) discoverWSURL() string {
	if n.account.CloudHost == "" {
		return n.tryDiscoveryEndpoint("https://internal.cloud.remarkable.com")
	}
	host := n.account.CloudHost
	wsScheme := "wss"
	if strings.HasPrefix(host, "http://") {
		wsScheme = "ws"
	}
	parsed, err := url.Parse(host)
	if err != nil {
		return ""
	}
	discovered := n.tryDiscoveryEndpoint(host)
	if discovered != "" {
		return discovered
	}
	return fmt.Sprintf("%s://%s/notifications/ws/json/1", wsScheme, parsed.Host)
}

func (n *Notifier) tryDiscoveryEndpoint(baseURL string) string {
	type endpoints struct {
		Notifications string `json:"notifications"`
	}
	discoveryURL := baseURL + "/discovery/v1/endpoints"
	req, err := http.NewRequest("GET", discoveryURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+n.account.HttpCtx.Tokens.UserToken)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return ""
	}
	defer resp.Body.Close()
	var ep endpoints
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil || ep.Notifications == "" {
		return ""
	}
	wsScheme := "wss"
	if strings.HasPrefix(ep.Notifications, "http://") {
		wsScheme = "ws"
	}
	parsed, err := url.Parse(ep.Notifications)
	if err != nil {
		return ""
	}
	host := parsed.Host
	if host == "" {
		host = ep.Notifications
	}
	return fmt.Sprintf("%s://%s/notifications/ws/json/1", wsScheme, host)
}

func (n *Notifier) runWebSocket(wsURL string) {
	backoff := time.Second
	maxBackoff := 2 * time.Minute

	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		}
		header := http.Header{}
		header.Set("Authorization", "Bearer "+n.account.HttpCtx.Tokens.UserToken)

		conn, _, err := dialer.Dial(wsURL, header)
		if err != nil {
			log.Printf("[%s] WebSocket connect failed: %v, falling back to polling", n.account.Name, err)
			n.runPolling()
			return
		}

		log.Printf("[%s] WebSocket connected", n.account.Name)
		backoff = time.Second

		n.pollOnce()

		err = n.readMessages(conn)
		conn.Close()
		if err != nil {
			log.Printf("[%s] WebSocket error: %v, reconnecting in %v", n.account.Name, err, backoff)
		}

		select {
		case <-n.stopCh:
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (n *Notifier) readMessages(conn *websocket.Conn) error {
	for {
		select {
		case <-n.stopCh:
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var msg wsMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[%s] unparseable WS message: %s", n.account.Name, string(message))
			continue
		}

		event := msg.Message.Attributes["event"]
		if event == "SyncComplete" {
			log.Printf("[%s] SyncComplete notification received", n.account.Name)
			n.onChange()
		}
	}
}

func (n *Notifier) runPolling() {
	lastHash, lastGen, _ := n.account.RootHash()

	ticker := time.NewTicker(time.Duration(n.pollSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			hash, gen, err := n.account.RootHash()
			if err != nil {
				log.Printf("[%s] poll error: %v", n.account.Name, err)
				continue
			}
			if hash != lastHash || gen != lastGen {
				log.Printf("[%s] change detected (gen %d→%d)", n.account.Name, lastGen, gen)
				lastHash = hash
				lastGen = gen
				n.onChange()
			}
		}
	}
}

func (n *Notifier) pollOnce() {
	hash, gen, err := n.account.RootHash()
	if err == nil {
		log.Printf("[%s] initial state: gen=%d hash=%s", n.account.Name, gen, hash[:8])
	}
}
