package noderunner_test

import (
	"bytes"
	"context"
	"fmt"
	. "github.com/dracory/noderun"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

// freePort is defined in engine_test.go.

// submitWorkflow builds a submitIn -> held queue flow so the emitted
// message can be inspected via the queue's staging list.
func submitWorkflow(t *testing.T, id, path string, port int, data map[string]interface{}) *Workflow {
	cfg := map[string]interface{}{
		"path": path,
		"port": port,
	}
	for k, v := range data {
		cfg[k] = v
	}
	return &Workflow{
		ID:   id,
		Name: "Submit Flow",
		Nodes: []Node{
			{ID: "sub", Type: "submitIn", Data: cfg},
			// Held dir-backend queue: stages the emitted message so the
			// test can inspect it (the durable backend needs a store
			// tests do not wire).
			{ID: "q", Type: "queue", Data: map[string]interface{}{
				"hold": true, "backend": "dir", "dir": t.TempDir(),
			}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "sub", Target: "q"},
		},
	}
}

// awaitQueueItem polls until the queue holds one item (or times out).
func awaitQueueItem(t *testing.T, engine *Engine, wfID string) QueueItem {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if q, ok := engine.QueueHandle(wfID, "q"); ok {
			if items := q.Items(); len(items) > 0 {
				return items[0]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no item reached the queue")
	return QueueItem{}
}

// multipartBody builds a multipart/form-data request body with one
// file part (field "file").
func multipartBody(t *testing.T, filename, contentType string, content []byte) (body *bytes.Buffer, ct string) {
	t.Helper()
	body = &bytes.Buffer{}
	w := multipart.NewWriter(body)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	h.Set("Content-Type", contentType)
	part, err := w.CreatePart(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return body, w.FormDataContentType()
}

// TestSubmitMultipartEmits — a multipart POST emits a message whose
// payload is the file bytes and whose msg.submit carries the full
// job-ticket metadata.
func TestSubmitMultipartEmits(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := submitWorkflow(t, "test-submit-multipart", "/submit/x", port, nil)

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	content := []byte("%PDF-1.4 fake job")
	body, ct := multipartBody(t, "job.pdf", "application/pdf", content)
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/submit/x", port), ct, body)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	item := awaitQueueItem(t, engine, wf.ID)
	data, ok := item.Payload.GetPayload().([]byte)
	if !ok {
		t.Fatalf("payload is %T, expected []byte", item.Payload.GetPayload())
	}
	if !bytes.Equal(data, content) {
		t.Fatalf("payload bytes differ: %q", data)
	}

	submit, ok := item.Payload.GetMetadata()["submit"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing msg.submit metadata: %v", item.Payload.GetMetadata())
	}
	if submit["filename"] != "job.pdf" {
		t.Fatalf("filename = %v", submit["filename"])
	}
	if submit["field"] != "file" {
		t.Fatalf("field = %v", submit["field"])
	}
	if submit["contentType"] != "application/pdf" {
		t.Fatalf("contentType = %v", submit["contentType"])
	}
	if submit["size"] != int64(len(content)) {
		t.Fatalf("size = %v", submit["size"])
	}
	if submit["submittedBy"] != "" {
		t.Fatalf("submittedBy should be empty with auth off, got %v", submit["submittedBy"])
	}
	if submit["submittedAt"] == "" || submit["submittedAt"] == nil {
		t.Fatal("submittedAt missing")
	}
	if submit["remoteAddr"] == "" || submit["remoteAddr"] == nil {
		t.Fatal("remoteAddr missing")
	}
	if item.Payload.GetMetadata()["msgId"] == nil {
		t.Fatal("msgId not stamped")
	}
}

// TestSubmitRawBody — a non-multipart POST carries the raw body with
// the ?filename= hint and the request's content type.
func TestSubmitRawBody(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := submitWorkflow(t, "test-submit-raw", "/submit", port, nil)

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/submit?filename=hello.txt", port),
		"text/plain",
		strings.NewReader("hello world"),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	item := awaitQueueItem(t, engine, wf.ID)
	data, _ := item.Payload.GetPayload().([]byte)
	if string(data) != "hello world" {
		t.Fatalf("payload = %q", data)
	}
	submit := item.Payload.GetMetadata()["submit"].(map[string]interface{})
	if submit["filename"] != "hello.txt" {
		t.Fatalf("filename = %v", submit["filename"])
	}
	if submit["contentType"] != "text/plain" {
		t.Fatalf("contentType = %v", submit["contentType"])
	}
}

// TestSubmitAuth — with auth "admin" the authenticator's username
// lands in msg.submit.submittedBy; unauthenticated requests get a 401
// Basic challenge.
func TestSubmitAuth(t *testing.T) {
	engine := NewEngine()
	engine.HTTPAuthenticator = func(r *http.Request) (string, bool) {
		user, pass, ok := r.BasicAuth()
		if !ok || pass != "s3cret" {
			return "", false
		}
		return user, true
	}
	port := freePort(t)
	wf := submitWorkflow(t, "test-submit-auth", "/submit/x", port, map[string]interface{}{"auth": "admin"})

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%d/submit/x", port)

	// No credentials -> 401 challenge
	body, ct := multipartBody(t, "a.txt", "text/plain", []byte("a"))
	resp, err := http.Post(url, ct, body)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("expected WWW-Authenticate challenge")
	}

	// Authenticated -> 200 and submittedBy stamped
	body, ct = multipartBody(t, "job.bin", "application/octet-stream", []byte("bin"))
	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", ct)
	req.SetBasicAuth("alice", "s3cret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	item := awaitQueueItem(t, engine, wf.ID)
	submit := item.Payload.GetMetadata()["submit"].(map[string]interface{})
	if submit["submittedBy"] != "alice" {
		t.Fatalf("submittedBy = %v, want alice", submit["submittedBy"])
	}
}

// TestSubmitOversize — an upload larger than maxSize is rejected with
// a 413 and a clear error.
func TestSubmitOversize(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := submitWorkflow(t, "test-submit-oversize", "/submit/x", port, map[string]interface{}{
		"maxSize": 64,
	})

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	body, ct := multipartBody(t, "big.bin", "application/octet-stream", []byte(strings.Repeat("x", 4096)))
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/submit/x", port), ct, body)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

// TestSubmitOversizeRaw — Content-Length past the cap is rejected
// before the body is even read (raw POST path).
func TestSubmitOversizeRaw(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := submitWorkflow(t, "test-submit-oversize-raw", "/submit", port, map[string]interface{}{
		"maxSize": 8,
	})

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/submit", port),
		"application/octet-stream", strings.NewReader("way more than eight bytes"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

// TestSubmitValidate — config sanity: bad auth and bad paths fail
// Validate, defaults normalize.
func TestSubmitValidate(t *testing.T) {
	n := NewSubmitNode("s", &SubmitConfig{Auth: "bogus"}, nil, "wf")
	if err := n.Validate(); err == nil {
		t.Fatal("invalid auth must fail Validate")
	}

	n = NewSubmitNode("s", &SubmitConfig{Path: "/a/:id/:id"}, nil, "wf")
	if err := n.Validate(); err == nil {
		t.Fatal("duplicate wildcard path must fail Validate")
	}

	n = NewSubmitNode("s", &SubmitConfig{}, nil, "wf")
	if err := n.Validate(); err != nil {
		t.Fatalf("empty config must normalize, got %v", err)
	}
	// Default normalization (path /submit, maxSize cap) is covered by
	// the internal test in nodebase/submitin.
}

// TestSubmitMainListener — a submitIn with listener "main" accepts
// uploads on the shared router's mux (POST only) and unregisters on
// stop; it fails activation when the engine has no router.
func TestSubmitMainListener(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf := submitWorkflow(t, "test-submit-main", "/up", 0,
		map[string]interface{}{"listener": "main"})
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	body, ct := multipartBody(t, "a.txt", "text/plain", []byte("hi"))
	resp, err := http.Post(srv.URL+"/up", ct, body)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 receipt, got %d", resp.StatusCode)
	}
	awaitQueueItem(t, engine, wf.ID)

	// GET is rejected by the method-qualified pattern.
	resp, err = http.Get(srv.URL + "/up")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

// TestSubmitMainListenerRequiresRouter — no shared router, no fallback.
func TestSubmitMainListenerRequiresRouter(t *testing.T) {
	engine := NewEngine()
	wf := submitWorkflow(t, "test-submit-norouter", "/up", 0,
		map[string]interface{}{"listener": "main"})
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err == nil {
		_ = engine.StopWorkflow(wf.ID)
		t.Fatal("expected deploy failure without engine.HTTPRouter")
	}
}
