package noderunner_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	. "github.com/dracory/noderun"
	"testing"
)

// TestHTTPRequestJSONWithCharset — BUG-1: a Content-Type of
// "application/json; charset=utf-8" must still parse the body as JSON.
func TestHTTPRequestJSONWithCharset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: srv.URL, Method: "GET"})
	out, err := n.Execute(context.Background(), &Message{})
	if err != nil {
		t.Fatal(err)
	}
	body := out.GetPayload().(map[string]interface{})["body"]
	m, ok := body.(map[string]interface{})
	if !ok {
		t.Fatalf("expected parsed JSON body, got %T", body)
	}
	if m["ok"] != true {
		t.Fatalf("expected ok=true, got %v", m["ok"])
	}
}

// TestHTTPRequestTypedJSON — BUG-1: application/*+json media types are
// JSON too.
func TestHTTPRequestTypedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/hal+json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: srv.URL, Method: "GET"})
	out, err := n.Execute(context.Background(), &Message{})
	if err != nil {
		t.Fatal(err)
	}
	body := out.GetPayload().(map[string]interface{})["body"]
	if _, ok := body.(map[string]interface{}); !ok {
		t.Fatalf("expected parsed JSON body, got %T", body)
	}
}

// TestHTTPRequestStringBodyRaw — BUG-2: a string payload must be sent
// raw, not JSON-quoted.
func TestHTTPRequestStringBodyRaw(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: srv.URL, Method: "POST"})
	if _, err := n.Execute(context.Background(), &Message{Payload: "hello"}); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("expected raw body hello, got %q", got)
	}
}

// TestHTTPRequestGetSendsNoBody — BUG-2: GET requests carry no body.
func TestHTTPRequestGetSendsNoBody(t *testing.T) {
	got := make(chan int, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- len(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: srv.URL, Method: "GET"})
	if _, err := n.Execute(context.Background(), &Message{Payload: "hello"}); err != nil {
		t.Fatal(err)
	}
	if n := <-got; n != 0 {
		t.Fatalf("expected empty body on GET, got %d bytes", n)
	}
}

// TestHTTPRequestSchemeRestriction — SEC-3: only http/https URLs are
// allowed, whether from config or the message.
func TestHTTPRequestSchemeRestriction(t *testing.T) {
	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: "file:///etc/passwd", Method: "GET"})
	if _, err := n.Execute(context.Background(), &Message{}); err == nil {
		t.Fatal("expected scheme rejection for file:// URL")
	}

	n = NewHTTPRequest("h", &HTTPRequestConfig{Method: "GET"})
	input := &Message{Payload: map[string]interface{}{"url": "ftp://example.com/x"}}
	if _, err := n.Execute(context.Background(), input); err == nil {
		t.Fatal("expected scheme rejection for message-supplied ftp:// URL")
	}
}

// TestHTTPRequestRedirectSchemeGate — N-7: redirects must pass the same
// scheme gate as the initial URL; a redirect to a non-HTTP scheme is
// refused.
func TestHTTPRequestRedirectSchemeGate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "file:///etc/passwd")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	n := NewHTTPRequest("h", &HTTPRequestConfig{URL: srv.URL, Method: "GET"})
	if _, err := n.Execute(context.Background(), &Message{}); err == nil {
		t.Fatal("expected redirect to file:// to be refused")
	}

	// Ordinary http redirects still work.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	hop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusFound)
	}))
	defer hop.Close()

	n = NewHTTPRequest("h", &HTTPRequestConfig{URL: hop.URL, Method: "GET"})
	out, err := n.Execute(context.Background(), &Message{})
	if err != nil {
		t.Fatalf("http redirect should succeed: %v", err)
	}
	if resp, ok := out.GetPayload().(map[string]interface{}); !ok || resp["statusCode"] != 200 {
		t.Fatalf("expected 200 after redirect, got %v", out.GetPayload())
	}
}
