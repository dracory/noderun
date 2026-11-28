package noderunner_test

import (
	"context"
	"fmt"
	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// freePort is defined in engine_test.go.

// triggerWorkflow builds a minimal flow: httpTrigger -> httpResponse so
// requests can be answered synchronously in tests.
func triggerWorkflow(id string, path string) *Workflow {
	return &Workflow{
		ID:   id,
		Name: "Trigger Flow",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "POST", "path": path, "port": 0,
			}},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", Target: "resp"},
		},
	}
}

// TestHTTPTriggerBodyLimit — SEC-2: a body larger than
// maxTriggerBodyBytes is rejected with 413.
func TestHTTPTriggerBodyLimit(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := triggerWorkflow("test-trigger-limit", "/hook")
	wf.Nodes[0].Data["port"] = port

	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	big := strings.Repeat("x", maxTriggerBodyBytes+1024)
	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/hook", port),
		"application/json",
		strings.NewReader(big),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

// maxTriggerBodyBytes mirrors the unexported trigger body cap.
const maxTriggerBodyBytes = 1 << 20

// TestHTTPTriggerRouteParams — NR-25: ":name" path segments land in
// msg.req.params.
func TestHTTPTriggerRouteParams(t *testing.T) {
	// Exercise the mux conversion directly — no listener needed.
	pattern, params := nodebase.MuxPattern("/user/:id/orders/:orderID")
	if pattern != "/user/{id}/orders/{orderID}" {
		t.Fatalf("unexpected pattern %q", pattern)
	}
	if len(params) != 2 || params[0] != "id" || params[1] != "orderID" {
		t.Fatalf("unexpected params %v", params)
	}

	// And a full request against a real listener.
	engine := NewEngine()
	port := freePort(t)
	wf := &Workflow{
		ID:   "test-trigger-params",
		Name: "Params Flow",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "GET", "path": "/user/:id", "port": port,
			}},
			{ID: "echo", Type: "function", Data: map[string]interface{}{
				// Mutate and return msg so the msgId metadata survives
				// for the httpResponse correlation.
				"func": "msg.payload = msg.req.params.id; return msg;",
			}},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", Target: "echo"},
			{ID: "e2", Source: "echo", Target: "resp"},
		},
	}
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	waitForListener(t, port, 5*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/user/42", port))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// TestHTTPTriggerConcurrentStop — R3-2: Stop() must be idempotent and
// race-free when called from two goroutines at once (StopWorkflow vs a
// mid-activation cancel). The listener must still be released.
func TestHTTPTriggerConcurrentStop(t *testing.T) {
	port := freePort(t)
	n := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "/x", Port: port,
	}, nil, "wf")
	if _, err := n.Execute(context.Background(), &Message{}); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- n.Stop()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Stop returned error: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 100*time.Millisecond)
		if err != nil {
			return // listener released
		}
		_ = c.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("listener still accepting after Stop")
}

// TestHTTPTriggerBadPathDoesNotDeadlock — R4-1: a duplicate ":name"
// route parameter must fail Execute (and deploy-time Validate) instead
// of panicking inside the locked section, leaving Stop() hanging.
func TestHTTPTriggerBadPathDoesNotDeadlock(t *testing.T) {
	n := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "/a/:id/:id", Port: freePort(t),
	}, nil, "wf")

	if err := n.Validate(); err == nil {
		t.Fatal("duplicate wildcard path must fail Validate")
	}
	if _, err := n.Execute(context.Background(), &Message{}); err == nil {
		t.Fatal("expected Execute error for duplicate wildcard path")
	}

	done := make(chan error, 1)
	go func() { done <- n.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked — serverMu leaked")
	}
}

// TestHTTPTriggerInvalidPathDoesNotDeadlock — R4-1: an invalid config
// that fails Validate must not leave the mutex held either.
func TestHTTPTriggerInvalidPathDoesNotDeadlock(t *testing.T) {
	n := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "", Port: freePort(t),
	}, nil, "wf")

	if _, err := n.Execute(context.Background(), &Message{}); err == nil {
		t.Fatal("expected Execute error for empty path")
	}

	done := make(chan error, 1)
	go func() { done <- n.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked — serverMu leaked")
	}
}

// TestHTTPTriggerPathNormalizationShared — Validate and Execute must use
// the same route pattern: a path without a leading slash is valid and
// serves under "/", while a duplicate ":name" is rejected at deploy.
func TestHTTPTriggerPathNormalizationShared(t *testing.T) {
	ok := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "api/:id", Port: freePort(t),
	}, nil, "wf")
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid relative path rejected: %v", err)
	}
	if _, err := ok.Execute(context.Background(), &Message{}); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	t.Cleanup(func() { _ = ok.Stop() })

	bad := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "a/:x/:x", Port: freePort(t),
	}, nil, "wf")
	if err := bad.Validate(); err == nil {
		t.Fatal("duplicate wildcard via relative path must fail Validate")
	}
}

// ---- listener "main" (shared HTTPRouter seam) ----

// sharedTestRouter is a minimal flowrunner.HTTPRouter for
// main-listener tests: copy-on-write over a ServeMux, mirroring the
// flowadmin adapter (which cannot be imported — flowadmin depends on
// flowrunner).
type sharedTestRouter struct {
	mu     sync.Mutex
	routes map[string]http.Handler
	mux    *http.ServeMux
}

func newSharedTestRouter() *sharedTestRouter {
	return &sharedTestRouter{routes: map[string]http.Handler{}, mux: http.NewServeMux()}
}

func (r *sharedTestRouter) Handle(pattern string, h http.Handler) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("route %q conflicts: %v", pattern, rec)
		}
	}()
	next := http.NewServeMux()
	for p, hh := range r.routes {
		next.Handle(p, hh)
	}
	next.Handle(pattern, h)
	r.routes[pattern] = h
	r.mux = next
	return nil
}

func (r *sharedTestRouter) Unregister(pattern string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, pattern)
	next := http.NewServeMux()
	for p, hh := range r.routes {
		next.Handle(p, hh)
	}
	r.mux = next
}

func (r *sharedTestRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	mux := r.mux
	r.mu.Unlock()
	mux.ServeHTTP(w, req)
}

// triggerMainWorkflow builds a httpTrigger -> httpResponse flow with
// listener "main" plus any data overrides.
func triggerMainWorkflow(id string, overrides map[string]interface{}) *Workflow {
	d := map[string]interface{}{
		"method": "POST", "path": "/hook", "listener": "main",
	}
	for k, v := range overrides {
		d[k] = v
	}
	return &Workflow{
		ID:   id,
		Name: "Main Listener Flow",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: d},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", Target: "resp"},
		},
	}
}

// TestHTTPTriggerMainListenerServes — a main-mode trigger answers
// requests on the shared router's mux at its verbatim path, and the
// method-qualified pattern rejects other methods with 405.
func TestHTTPTriggerMainListenerServes(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf := triggerMainWorkflow("test-main-serve", nil)
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	resp, err := http.Post(srv.URL+"/hook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Method-qualified pattern: GET on a POST route is a 405.
	resp, err = http.Get(srv.URL + "/hook")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

// TestHTTPTriggerMainListenerRouteParams — ":name" wildcards still land
// in msg.req.params when served through the shared router.
func TestHTTPTriggerMainListenerRouteParams(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf := triggerMainWorkflow("test-main-params", map[string]interface{}{
		"method": "GET", "path": "/user/:id",
	})
	wf.Nodes = append(wf.Nodes, Node{ID: "echo", Type: "function", Data: map[string]interface{}{
		"func": "msg.payload = msg.req.params.id; return msg;",
	}})
	wf.Edges = []Edge{
		{ID: "e1", Source: "trig", Target: "echo"},
		{ID: "e2", Source: "echo", Target: "resp"},
	}
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	resp, err := http.Get(srv.URL + "/user/42")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d (%s)", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "42") {
		t.Fatalf("expected echoed param in body, got %s", body)
	}
}

// TestHTTPTriggerMainListenerRequiresRouter — a main-mode trigger on an
// engine with no shared router fails activation explicitly (no silent
// fallback to a private port).
func TestHTTPTriggerMainListenerRequiresRouter(t *testing.T) {
	engine := NewEngine()
	wf := triggerMainWorkflow("test-main-norouter", nil)
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err == nil {
		_ = engine.StopWorkflow(wf.ID)
		t.Fatal("expected deploy failure without engine.HTTPRouter")
	} else if !strings.Contains(err.Error(), "HTTPRouter") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestHTTPTriggerMainListenerConflict — two main-mode triggers on the
// same method+path: the second registrant's deploy fails while the
// first keeps serving.
func TestHTTPTriggerMainListenerConflict(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf1 := triggerMainWorkflow("test-main-first", nil)
	if _, err := engine.DeployWorkflow(context.Background(), wf1, nil); err != nil {
		t.Fatalf("first deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf1.ID) }()

	wf2 := triggerMainWorkflow("test-main-second", nil)
	if _, err := engine.DeployWorkflow(context.Background(), wf2, nil); err == nil {
		_ = engine.StopWorkflow(wf2.ID)
		t.Fatal("expected conflict to fail the second deploy")
	}

	// The first mount is untouched.
	resp, err := http.Post(srv.URL+"/hook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("first trigger lost its route: %d", resp.StatusCode)
	}
}

// TestHTTPTriggerMainListenerUnregister — stopping the workflow
// unregisters the route (404 afterwards).
func TestHTTPTriggerMainListenerUnregister(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf := triggerMainWorkflow("test-main-unmount", nil)
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	if err := engine.StopWorkflow(wf.ID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	resp, err := http.Post(srv.URL+"/hook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after stop, got %d", resp.StatusCode)
	}
}

// TestHTTPTriggerMainListenerPartialDeploy — an unchanged trigger keeps
// its shared-route registration across a partial redeploy; a dirty
// trigger remounts (old path 404s, new path serves).
func TestHTTPTriggerMainListenerPartialDeploy(t *testing.T) {
	engine := NewEngine()
	router := newSharedTestRouter()
	engine.HTTPRouter = router
	srv := httptest.NewServer(router)
	defer srv.Close()

	wf := triggerMainWorkflow("test-main-partial", nil)
	// A second, unrelated node so a partial deploy has something to
	// dirty that is not the trigger.
	wf.Nodes = append(wf.Nodes, Node{ID: "dbg", Type: "debug", Data: map[string]interface{}{"name": "a"}})
	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	// Partial redeploy dirtying only the debug node: the trigger's
	// mount must survive (a remount would conflict and fail).
	wf2 := *wf
	wf2.Nodes = append([]Node(nil), wf.Nodes...)
	wf2.Nodes[2].Data = map[string]interface{}{"name": "b"}
	if _, err := engine.DeployWorkflowPartial(context.Background(), &wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}
	resp, err := http.Post(srv.URL+"/hook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("unchanged trigger lost its route on partial redeploy: %d", resp.StatusCode)
	}

	// Partial redeploy changing the trigger path: remount happens.
	wf3 := wf2
	wf3.Nodes = append([]Node(nil), wf2.Nodes...)
	trig := wf3.Nodes[0]
	trig.Data = map[string]interface{}{
		"method": "POST", "path": "/hook2", "listener": "main",
	}
	wf3.Nodes[0] = trig
	if _, err := engine.DeployWorkflowPartial(context.Background(), &wf3, nil); err != nil {
		t.Fatalf("partial redeploy (path change) failed: %v", err)
	}
	resp, err = http.Post(srv.URL+"/hook", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("old route still served after remount: %d", resp.StatusCode)
	}
	resp, err = http.Post(srv.URL+"/hook2", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp = requireHTTPResponse(t, resp, err)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("new route not serving after remount: %d", resp.StatusCode)
	}
}

// TestHTTPTriggerInvalidListener — a listener value outside
// own|main fails Validate (and therefore deploy).
func TestHTTPTriggerInvalidListener(t *testing.T) {
	n := NewHTTPTrigger("t", &HTTPTriggerConfig{
		Method: "GET", Path: "/x", Port: freePort(t), Listener: "bogus",
	}, nil, "wf")
	if err := n.Validate(); err == nil {
		t.Fatal("expected invalid listener to fail Validate")
	}
}
