package noderunner

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"sync"
	"testing"
	"time"

	imapbackendmemory "github.com/emersion/go-imap/backend/memory"
	imapserver "github.com/emersion/go-imap/server"
)

// ---------------------------------------------------------------------------
// Fake SMTP sink
// ---------------------------------------------------------------------------

// fakeSMTPSink is a minimal SMTP server: it accepts any envelope and
// records the DATA block of each delivered message.
type fakeSMTPSink struct {
	listener net.Listener
	// authMechs, when non-empty, is advertised as "AUTH <mechs>" in
	// the EHLO reply and AUTH commands run the scripted exchange.
	authMechs string
	mu        sync.Mutex
	mails     []string // raw DATA blocks
	rcpts     []string // all RCPT TO args seen
	authLog   []string // client lines received during an AUTH exchange
}

func startFakeSMTP(t *testing.T) *fakeSMTPSink {
	return startFakeSMTPWithAuth(t, "")
}

// startFakeSMTPWithAuth starts the sink advertising mechs (e.g.
// "LOGIN") as its AUTH extension.
func startFakeSMTPWithAuth(t *testing.T, mechs string) *fakeSMTPSink {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake smtp listen: %v", err)
	}
	s := &fakeSMTPSink{listener: l, authMechs: mechs}
	go s.serve()
	t.Cleanup(func() { _ = l.Close() })
	return s
}

func (s *fakeSMTPSink) addr() (string, int) {
	host, portStr, _ := net.SplitHostPort(s.listener.Addr().String())
	port, _ := net.LookupPort("tcp", portStr)
	return host, port
}

func (s *fakeSMTPSink) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeSMTPSink) handle(conn net.Conn) {
	defer conn.Close()
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)
	reply := func(format string, args ...interface{}) {
		fmt.Fprintf(w, format+"\r\n", args...)
		_ = w.Flush()
	}
	reply("220 fake-smtp ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		upper := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
			reply("250-fake-smtp greets you")
			if s.authMechs != "" {
				reply("250-AUTH %s", s.authMechs)
			}
			reply("250 8BITMIME")
		case strings.HasPrefix(upper, "AUTH"):
			// Scripted LOGIN-style exchange: challenge for the
			// username, then the password; every client answer is
			// logged in authLog.
			reply("334 %s", base64.StdEncoding.EncodeToString([]byte("Username:")))
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			s.mu.Lock()
			s.authLog = append(s.authLog, strings.TrimSpace(line))
			s.mu.Unlock()
			reply("334 %s", base64.StdEncoding.EncodeToString([]byte("Password:")))
			line, err = r.ReadString('\n')
			if err != nil {
				return
			}
			s.mu.Lock()
			s.authLog = append(s.authLog, strings.TrimSpace(line))
			s.mu.Unlock()
			reply("235 2.7.0 Authentication successful")
		case strings.HasPrefix(upper, "MAIL FROM"):
			reply("250 OK")
		case strings.HasPrefix(upper, "RCPT TO"):
			s.mu.Lock()
			s.rcpts = append(s.rcpts, line)
			s.mu.Unlock()
			reply("250 OK")
		case strings.HasPrefix(upper, "DATA"):
			reply("354 End data with <CR><LF>.<CR><LF>")
			var body strings.Builder
			for {
				dl, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if dl == ".\r\n" || dl == ".\n" {
					break
				}
				body.WriteString(dl)
			}
			s.mu.Lock()
			s.mails = append(s.mails, body.String())
			s.mu.Unlock()
			reply("250 OK queued")
		case strings.HasPrefix(upper, "QUIT"):
			reply("221 Bye")
			return
		default:
			reply("250 OK")
		}
	}
}

func (s *fakeSMTPSink) waitMails(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		if len(s.mails) >= n {
			out := append([]string{}, s.mails...)
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.mails...)
}

// ---------------------------------------------------------------------------
// Fake IMAP server (go-imap memory backend)
// ---------------------------------------------------------------------------

// startFakeIMAP serves the go-imap in-memory backend on a loopback
// port and returns its address + backend for appending test mail.
func startFakeIMAP(t *testing.T) (string, int, *imapbackendmemory.Backend) {
	t.Helper()
	be := imapbackendmemory.New()
	s := imapserver.New(be)
	s.AllowInsecureAuth = true
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake imap listen: %v", err)
	}
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { _ = s.Close() })
	host, portStr, _ := net.SplitHostPort(l.Addr().String())
	port, _ := net.LookupPort("tcp", portStr)
	return host, port, be
}

// appendMail drops a message into the fake mailbox via the backend.
func appendMail(t *testing.T, be *imapbackendmemory.Backend, subject, body string) {
	t.Helper()
	user, err := be.Login(nil, "username", "password")
	if err != nil {
		t.Fatalf("backend login: %v", err)
	}
	mbox, err := user.GetMailbox("INBOX")
	if err != nil {
		t.Fatalf("get INBOX: %v", err)
	}
	raw := "From: sender@example.org\r\n" +
		"To: rcpt@example.org\r\n" +
		"Subject: " + subject + "\r\n" +
		"Date: " + time.Now().Format("Mon, 02 Jan 2006 15:04:05 -0700") + "\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" + body
	if err := mbox.CreateMessage(nil, time.Now(), strings.NewReader(raw)); err != nil {
		t.Fatalf("append mail: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestMailConfigAndRegistration verifies config decoding, executor
// construction and the Stopper lifecycle for both node types.
func TestMailConfigAndRegistration(t *testing.T) {
	engine := NewEngine()
	in, err := engine.executorForNode("wf", &Node{ID: "mi", Type: "mailIn", Data: map[string]interface{}{
		"host": "imap.example.org", "username": "u", "password": "p",
	}})
	if err != nil {
		t.Fatalf("mailIn executorForNode failed: %v", err)
	}
	if _, ok := in.(Stopper); !ok {
		t.Fatal("mailIn must implement Stopper")
	}
	if err := in.Validate(); err != nil {
		t.Fatalf("mailIn validate failed: %v", err)
	}

	out, err := engine.executorForNode("wf", &Node{ID: "mo", Type: "mailOut", Data: map[string]interface{}{
		"host": "smtp.example.org", "from": "flow@example.org", "security": "plain",
	}})
	if err != nil {
		t.Fatalf("mailOut executorForNode failed: %v", err)
	}
	if err := out.Validate(); err != nil {
		t.Fatalf("mailOut validate failed: %v", err)
	}

	// Validation failures
	badIn, _ := engine.executorForNode("wf", &Node{ID: "mi2", Type: "mailIn", Data: map[string]interface{}{}})
	if verr := badIn.Validate(); verr == nil {
		t.Fatal("mailIn without host must fail validation")
	}
	badOut, _ := engine.executorForNode("wf", &Node{ID: "mo2", Type: "mailOut", Data: map[string]interface{}{
		"host": "h", "security": "bogus",
	}})
	if verr := badOut.Validate(); verr == nil {
		t.Fatal("mailOut with invalid security must fail validation")
	}
}

// TestMailOutSends executes a mailOut node against the fake SMTP sink
// and asserts the transmitted message.
func TestMailOutSends(t *testing.T) {
	sink := startFakeSMTP(t)
	host, port := sink.addr()

	node := NewMailOutNode("mo", &MailOutConfig{
		Host:     host,
		Port:     port,
		Security: "plain",
		From:     "flow@example.org",
		To:       "static@example.org",
		Subject:  "static subject",
	})
	input := &Message{Payload: "hello body", Timestamp: time.Now()}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("mailOut execute failed: %v", err)
	}
	if out != input {
		t.Fatal("mailOut must pass the input through")
	}

	mails := sink.waitMails(t, 1)
	if len(mails) != 1 {
		t.Fatalf("expected 1 mail at sink, got %d", len(mails))
	}
	got := mails[0]
	for _, want := range []string{
		"From: flow@example.org", "To: static@example.org",
		"Subject: static subject", "hello body",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("mail missing %q:\n%s", want, got)
		}
	}
}

// TestMailOutMessageOverrides verifies message metadata overrides:
// to/cc/bcc/subject via the mail object and the topic metadata, body
// from payload.
func TestMailOutMessageOverrides(t *testing.T) {
	sink := startFakeSMTP(t)
	host, port := sink.addr()

	node := NewMailOutNode("mo", &MailOutConfig{
		Host:     host,
		Port:     port,
		Security: "plain",
		From:     "flow@example.org",
		To:       "static@example.org",
		Subject:  "static subject",
	})
	input := &Message{
		Payload: "override body",
		Metadata: map[string]interface{}{
			"topic": "override subject",
			"mail": map[string]interface{}{
				"to": "a@x.org, b@x.org",
				"cc": "c@x.org",
			},
		},
		Timestamp: time.Now(),
	}
	if _, err := node.Execute(context.Background(), input); err != nil {
		t.Fatalf("mailOut execute failed: %v", err)
	}

	mails := sink.waitMails(t, 1)
	if len(mails) != 1 {
		t.Fatalf("expected 1 mail at sink, got %d", len(mails))
	}
	got := mails[0]
	for _, want := range []string{
		"To: a@x.org, b@x.org", "Cc: c@x.org",
		"Subject: override subject", "override body",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("mail missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "static@example.org") || strings.Contains(got, "static subject") {
		t.Fatalf("overridden values leaked:\n%s", got)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.rcpts) != 3 {
		t.Fatalf("expected 3 RCPT TO commands (to+to+cc), got %v", sink.rcpts)
	}
}

// TestMailOutAuthLogin sends through a sink advertising only AUTH
// LOGIN and asserts the base64 challenge/response exchange carried the
// configured credentials.
func TestMailOutAuthLogin(t *testing.T) {
	sink := startFakeSMTPWithAuth(t, "LOGIN")
	host, port := sink.addr()

	node := NewMailOutNode("mo", &MailOutConfig{
		Host:     host,
		Port:     port,
		Security: "plain",
		From:     "flow@example.org",
		To:       "static@example.org",
		Subject:  "login auth",
		Username: "mailuser",
		Password: "mailsecret",
	})
	input := &Message{Payload: "auth body", Timestamp: time.Now()}
	if _, err := node.Execute(context.Background(), input); err != nil {
		t.Fatalf("mailOut execute failed: %v", err)
	}

	mails := sink.waitMails(t, 1)
	if len(mails) != 1 {
		t.Fatalf("expected 1 mail at sink, got %d", len(mails))
	}

	sink.mu.Lock()
	log := append([]string{}, sink.authLog...)
	sink.mu.Unlock()
	want := []string{
		base64.StdEncoding.EncodeToString([]byte("mailuser")),
		base64.StdEncoding.EncodeToString([]byte("mailsecret")),
	}
	if len(log) != 2 || log[0] != want[0] || log[1] != want[1] {
		t.Fatalf("AUTH LOGIN exchange mismatch: got %v, want base64 responses %v", log, want)
	}
}

// TestMailInFetchesMails deploys mailIn -> debug against the fake IMAP
// server with two unseen mails and asserts both arrive downstream with
// subject in metadata and body as payload.
func TestMailInFetchesMails(t *testing.T) {
	host, port, be := startFakeIMAP(t)
	appendMail(t, be, "mail one", "body one")
	appendMail(t, be, "mail two", "body two")

	markSeen := true
	workflow := &Workflow{
		ID:   "test-mailin-flow",
		Name: "Mail In Flow",
		Nodes: []Node{
			{ID: "min", Type: "mailIn", Data: map[string]interface{}{
				"host": host, "port": float64(port), "security": "plain",
				"username": "username", "password": "password",
				"pollSeconds": 1, "markSeen": markSeen,
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"complete": "true"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "min", Target: "dbg"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		raw, _ := json.Marshal(events)
		text := string(raw)
		if strings.Count(text, "mail one") > 0 && strings.Count(text, "mail two") > 0 {
			if !strings.Contains(text, "body one") || !strings.Contains(text, "body two") {
				t.Fatalf("mail bodies missing from emitted messages:\n%s", text)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mails never reached debug node; events:\n%s", text)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMailInConnectFailsAtDeploy: an unreachable server surfaces as a
// deploy error, matching mqttIn.
func TestMailInConnectFailsAtDeploy(t *testing.T) {
	port := freeTCPPort(t) // closed by freeTCPPort — nothing listens
	workflow := &Workflow{
		ID:   "test-mailin-fail",
		Name: "Mail In Fail",
		Nodes: []Node{
			{ID: "min", Type: "mailIn", Data: map[string]interface{}{
				"host": "127.0.0.1", "port": float64(port), "security": "plain",
				"username": "u", "password": "p", "pollSeconds": 1,
			}},
		},
	}
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err == nil {
		_ = engine.StopWorkflow(workflow.ID)
		t.Fatal("deploy must fail when the IMAP server is unreachable")
	}
}

// TestMailCredentialsRegistered: mailIn/mailOut username+password are
// credential-typed so exports strip them.
func TestMailCredentialsRegistered(t *testing.T) {
	for _, typ := range []string{"mailIn", "mailOut"} {
		for _, key := range []string{"username", "password"} {
			if !nodestore.IsCredentialKey(typ, key) {
				t.Fatalf("%s.%s must be a credential key", typ, key)
			}
		}
	}

	w := &Workflow{
		ID:   "wf-mail-creds",
		Name: "mail creds",
		Nodes: []Node{
			{ID: "m1", Type: "mailOut", Data: map[string]interface{}{
				"host": "smtp.example.org", "username": "mailuser", "password": "mailsecret",
			}},
			{ID: "m2", Type: "mailIn", Data: map[string]interface{}{
				"host": "imap.example.org", "password": nodestore.CredentialSentinel,
			}},
		},
	}
	raw, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("ExportNodeRED: %v", err)
	}
	out := string(raw)
	for _, leak := range []string{"mailuser", "mailsecret", nodestore.CredentialSentinel} {
		if strings.Contains(out, leak) {
			t.Fatalf("export leaks %q: %s", leak, out)
		}
	}
}
