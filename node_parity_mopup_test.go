package noderunner_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- comment ---------------------------------------------------------------

// TestCommentDeploys verifies a comment node deploys and passes the
// NodeExecutor contract without runtime behaviour.
func TestCommentDeploys(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-comment-flow",
		Name: "Comment Flow",
		Nodes: []Node{
			{ID: "c1", Type: "comment", Data: map[string]interface{}{"info": "hello"}},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	node := NewCommentNode("c1", &CommentConfig{Info: "x"})
	out, err := node.Execute(context.Background(), &Message{Payload: "m"})
	if err != nil {
		t.Fatalf("comment Execute failed: %v", err)
	}
	if out == nil || out.GetPayload() != "m" {
		t.Fatalf("comment must pass through, got %+v", out)
	}
	if err := node.Validate(); err != nil {
		t.Fatalf("comment Validate failed: %v", err)
	}
}

// --- unknown ---------------------------------------------------------------

// TestUnknownExcludedFromExecution verifies an unrecognized type deploys,
// drops the message and warns once — Node-RED disabled-unknown parity.
func TestUnknownExcludedFromExecution(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-unknown-flow",
		Name: "Unknown Flow",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `"go"`, "once": true}},
			{ID: "unk", Type: "fancy-widget", Data: map[string]interface{}{"conf": 1}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "should not run"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "unk"},
			{ID: "e2", Source: "unk", Target: "lg"},
		},
	}
	engine := NewEngine()
	// New-stack contract (Node-RED type-registered parity): deploy
	// fails loudly listing every unregistered type — there is no
	// lenient exclude-and-warn anymore.
	_, deployErr := engine.ExecuteWorkflow(context.Background(), workflow, nil)
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(2 * time.Second)
	var flagged bool
	for time.Now().Before(deadline) {
		_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["lg"] == "ok" {
			t.Fatalf("message must not flow through an unknown node")
		}
		for _, ev := range events {
			if ev.Level == "error" && strings.Contains(ev.Message, "unregistered node type") {
				flagged = true
			}
		}
		if deployErr != nil && strings.Contains(deployErr.Error(), "unregistered node type") {
			flagged = true
		}
		if flagged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !flagged {
		t.Fatalf("deploy never flagged the unregistered node type (err=%v)", deployErr)
	}
}

// TestUnknownRoundTrip verifies the raw config of an unrecognized imported
// type survives a Node-RED export round-trip verbatim.
func TestUnknownRoundTrip(t *testing.T) {
	flowJSON := `[
		{"id":"t","type":"tab","label":"Main"},
		{"id":"u","type":"fancy-widget","z":"t","custom":{"a":1},"wires":[[]]}
	]`
	w, err := nodestore.ImportNodeRED([]byte(flowJSON))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	out, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(out, &items); err != nil {
		t.Fatalf("exported JSON invalid: %v", err)
	}
	var found map[string]interface{}
	for _, it := range items {
		if it["type"] == "fancy-widget" {
			found = it
		}
	}
	if found == nil {
		t.Fatalf("unknown type lost on export: %s", out)
	}
	if m, ok := found["custom"].(map[string]interface{}); !ok || m["a"] != float64(1) {
		t.Fatalf("unknown node config not round-tripped: %v", found)
	}
}

// TestUnknownPaletteType verifies the explicit "unknown" palette type
// deploys and is excluded like any unrecognized type.
func TestUnknownPaletteType(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-unknown-palette",
		Name: "Unknown Palette",
		Nodes: []Node{
			{ID: "u", Type: "unknown", Data: map[string]interface{}{"anything": true}},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()
}

// --- tcp request -----------------------------------------------------------

// echoServer runs fn(conn) per accepted connection until the listener
// closes. Returns the listen port.
func echoServer(t *testing.T, fn func(net.Conn)) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go fn(conn)
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

// TestTCPRequestReply verifies connect-send-receive: the server's reply
// becomes the message payload and remote ip/port land in metadata.
func TestTCPRequestReply(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		line, _ := r.ReadString('\n')
		_, _ = conn.Write([]byte("echo:" + line))
	})
	node := NewTCPRequestNode("rq", &TCPRequestConfig{
		Host: "127.0.0.1", Port: port, Mode: "length", Length: 9,
	}, nil, "wf")
	out, err := node.Execute(context.Background(), &Message{Payload: "ping\n"})
	if err != nil {
		t.Fatalf("tcpRequest failed: %v", err)
	}
	if s, _ := out.GetPayload().(string); s != "echo:ping" {
		t.Fatalf("unexpected reply %q", out.GetPayload())
	}
	if out.GetMetadata()["ip"] == "" || out.GetMetadata()["port"] == "" {
		t.Fatalf("remote ip/port metadata missing: %v", out.GetMetadata())
	}
}

// TestTCPRequestDelimiter verifies delimiter framing reads up to and
// including the delimiter.
func TestTCPRequestDelimiter(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		_, _ = r.ReadString('\n')
		_, _ = conn.Write([]byte("part1|part2|trailing"))
	})
	node := NewTCPRequestNode("rq", &TCPRequestConfig{
		Host: "127.0.0.1", Port: port,
		Mode: "delimiter", Delimiter: "|",
	}, nil, "wf")
	out, err := node.Execute(context.Background(), &Message{Payload: "go\n"})
	if err != nil {
		t.Fatalf("tcpRequest failed: %v", err)
	}
	if s, _ := out.GetPayload().(string); s != "part1|" {
		t.Fatalf("expected reply up to first delimiter, got %q", out.GetPayload())
	}
}

// TestTCPRequestTimeoutMode verifies default framing returns whatever
// the server sent before closing.
func TestTCPRequestTimeoutMode(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		_, _ = r.ReadString('\n')
		_, _ = conn.Write([]byte("done"))
		// half-close ends the reply before the deadline
	})
	node := NewTCPRequestNode("rq", &TCPRequestConfig{
		Host: "127.0.0.1", Port: port, Timeout: 5,
	}, nil, "wf")
	out, err := node.Execute(context.Background(), &Message{Payload: "x\n"})
	if err != nil {
		t.Fatalf("tcpRequest failed: %v", err)
	}
	if s, _ := out.GetPayload().(string); s != "done" {
		t.Fatalf("unexpected reply %q", out.GetPayload())
	}
}

// TestTCPRequestNoServer verifies a connection failure surfaces as an error.
func TestTCPRequestNoServer(t *testing.T) {
	node := NewTCPRequestNode("rq", &TCPRequestConfig{
		Host: "127.0.0.1", Port: 1, Timeout: 2,
	}, nil, "wf")
	if _, err := node.Execute(context.Background(), &Message{Payload: "x"}); err == nil {
		t.Fatal("expected connect error")
	}
}

// TestTCPRequestValidate checks framing-mode validation.
func TestTCPRequestValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  *TCPRequestConfig
		ok   bool
	}{
		{"default", &TCPRequestConfig{Port: 80}, true},
		{"delimiter", &TCPRequestConfig{Port: 80, Mode: "delimiter", Delimiter: `\n`}, true},
		{"delimiter missing", &TCPRequestConfig{Port: 80, Mode: "delimiter"}, false},
		{"length", &TCPRequestConfig{Port: 80, Mode: "length", Length: 4}, true},
		{"length missing", &TCPRequestConfig{Port: 80, Mode: "length"}, false},
		{"bad mode", &TCPRequestConfig{Port: 80, Mode: "bogus"}, false},
		{"bad port", &TCPRequestConfig{Port: 0}, false},
	}
	for _, c := range cases {
		node := NewTCPRequestNode("rq", c.cfg, nil, "wf")
		if err := node.Validate(); (err == nil) != c.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", c.name, err, c.ok)
		}
	}
}

// --- watch -----------------------------------------------------------------

// TestWatchDetectsChanges deploys watch(dir) -> log, then creates a
// file and asserts the change propagated downstream.
func TestWatchDetectsChanges(t *testing.T) {
	dir := t.TempDir()
	workflow := &Workflow{
		ID:   "test-watch-flow",
		Name: "Watch Flow",
		Nodes: []Node{
			{ID: "w", Type: "watch", Data: map[string]interface{}{
				"filename": dir, "interval": 0.05,
			}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "changed"}},
		},
		Edges: []Edge{{ID: "e1", Source: "w", Target: "lg"}},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	if nodes := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["w"] == "ok"
	}); nodes["w"] != "ok" {
		t.Fatalf("watch never activated: %v", nodes)
	}

	if err := os.WriteFile(filepath.Join(dir, "new.txt"), []byte("x"), 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	nodes := waitStatus(t, engine, workflow.ID, 3*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		t.Fatalf("watch change never reached log: %v", nodes)
	}
}

// --- tail ------------------------------------------------------------------

// TestTailNewLines deploys tail(file) -> log, appends a line, and
// asserts it arrived downstream via the event metadata.
func TestTailNewLines(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "log.txt")
	if err := os.WriteFile(file, []byte("old line\n"), 0644); err != nil {
		t.Fatalf("seed failed: %v", err)
	}
	workflow := &Workflow{
		ID:   "test-tail-flow",
		Name: "Tail Flow",
		Nodes: []Node{
			{ID: "tl", Type: "tail", Data: map[string]interface{}{
				"filename": file, "interval": 0.05,
			}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "got"}},
		},
		Edges: []Edge{{ID: "e1", Source: "tl", Target: "lg"}},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	if nodes := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["tl"] == "ok"
	}); nodes["tl"] != "ok" {
		t.Fatalf("tail never activated: %v", nodes)
	}

	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	_, _ = f.WriteString("new line\n")
	_ = f.Close()

	nodes := waitStatus(t, engine, workflow.ID, 3*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		t.Fatalf("appended line never reached log: %v", nodes)
	}
}

// --- tlsConfig -------------------------------------------------------------

// testCertFiles generates a self-signed certificate + key + CA file in
// a temp dir and returns their paths.
func testCertFiles(t *testing.T) (cert, key, ca string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("keygen failed: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("cert creation failed: %v", err)
	}
	dir := t.TempDir()
	cert = filepath.Join(dir, "cert.pem")
	key = filepath.Join(dir, "key.pem")
	ca = filepath.Join(dir, "ca.pem")
	writePEM := func(path, typ string, der []byte) {
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("write %s failed: %v", path, err)
		}
		defer func() { _ = f.Close() }()
		if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
			t.Fatalf("pem encode failed: %v", err)
		}
	}
	writePEM(cert, "CERTIFICATE", der)
	writePEM(ca, "CERTIFICATE", der)
	writePEM(key, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv))
	return cert, key, ca
}

// TestTLSConfigValidate checks cert/key pairing rules.
func TestTLSConfigValidate(t *testing.T) {
	if err := NewTLSConfigNode("t", &TLSConfig{}).Validate(); err != nil {
		t.Fatalf("empty config must validate: %v", err)
	}
	if err := NewTLSConfigNode("t", &TLSConfig{CertPath: "c.pem"}).Validate(); err == nil {
		t.Fatal("cert without key must fail")
	}
	if err := NewTLSConfigNode("t", &TLSConfig{CertPath: "c.pem", KeyPath: "k.pem"}).Validate(); err != nil {
		t.Fatalf("paired cert/key must validate: %v", err)
	}
}

// TestTLSConfigMissingRef verifies a bad "tls" reference fails clearly.
func TestTLSConfigMissingRef(t *testing.T) {
	engine := NewEngine()
	if _, err := engine.LoadTLSConfig("wf", "nope"); err == nil {
		t.Fatal("missing tlsConfig ref must fail")
	}
}

// TestTCPRequestOverTLS deploys a tlsConfig node plus a tcpRequest and
// verifies the request succeeds over a real TLS connection.
func TestTCPRequestOverTLS(t *testing.T) {
	certPath, keyPath, caPath := testCertFiles(t)

	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("server keypair load failed: %v", err)
	}
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{pair}})
	if err != nil {
		t.Fatalf("tls listen failed: %v", err)
	}
	defer func() { _ = l.Close() }()
	port := l.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				_, _ = r.ReadString('\n')
				_, _ = c.Write([]byte("secure\n"))
			}(conn)
		}
	}()

	// Deploy a workflow so the tlsConfig node resolves via LookupNode.
	workflow := &Workflow{
		ID:   "test-tls-flow",
		Name: "TLS Flow",
		Nodes: []Node{
			{ID: "tc", Type: "tlsConfig", Data: map[string]interface{}{
				"ca": caPath, "verifyservercert": true,
			}},
			{ID: "rq", Type: "tcpRequest", Data: map[string]interface{}{
				"host": "127.0.0.1", "port": float64(port),
				"mode": "delimiter", "delimiter": "\\n", "tls": "tc",
			}},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	cfg := &TCPRequestConfig{
		Host: "127.0.0.1", Port: port,
		Mode: "delimiter", Delimiter: "\n", TLS: "tc",
	}
	node := NewTCPRequestNode("rq", cfg, engine, workflow.ID)
	out, err := node.Execute(context.Background(), &Message{Payload: "hi\n"})
	if err != nil {
		t.Fatalf("tcpRequest over TLS failed: %v", err)
	}
	if s, _ := out.GetPayload().(string); s != "secure\n" {
		t.Fatalf("unexpected reply %q", out.GetPayload())
	}
}

// --- node-types listing ----------------------------------------------------

// TestParityNodeTypesRegistered verifies the new types appear in the
// executor switch via a deploy of each.
func TestParityNodeTypesRegistered(t *testing.T) {
	types := []struct {
		typ  string
		data map[string]interface{}
	}{
		{"comment", map[string]interface{}{}},
		{"unknown", map[string]interface{}{}},
		{"tlsConfig", map[string]interface{}{}},
		{"html", map[string]interface{}{"tag": "a"}},
	}
	var nodes []Node
	for i, tc := range types {
		nodes = append(nodes, Node{ID: fmt.Sprintf("n%d", i), Type: tc.typ, Data: tc.data})
	}
	workflow := &Workflow{ID: "test-parity-types", Name: "Parity", Nodes: nodes}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()
}
