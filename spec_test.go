// Package spec ports Node-RED's own mocha spec assertions
// (test/nodes/**_spec.js at the pinned upstream tag, see
// cmd/speccoverage) into Go tests.
//
// TITLE DISCIPLINE is the mechanism: every ported assertion is a
// subtest whose title is copied VERBATIM from the upstream
// describe()/it() strings, so the title itself is the join key for
// the coverage diff generator. Because Go sanitizes subtest names
// (spaces etc.), subtests are registered through describe()/it()
// below, which (a) maps each title through sanitizeName for the Go
// test name and (b) records the ORIGINAL title plus the outcome
// (covered | skipped | failed) in an in-memory registry. When the
// environment variable GOFLOW_SPEC_MANIFEST points at a file,
// TestSpecManifest (zz_manifest_test.go, always last) dumps that
// registry as JSON for the generator.
//
// Title mapping convention (sanitizeName): any run of characters
// outside [a-zA-Z0-9_] collapses to a single "_" and leading/trailing
// "_" are trimmed, so `it('should check if payload if of type string ')`
// becomes `should_check_if_payload_if_of_type_string`. The registry —
// not the mangled Go name — carries the verbatim title.
//
// Honesty rule (from the task page): "covered" means the upstream
// assertion is ported and passes. The generator joins on titles only;
// port fidelity is the reviewer's concern.
package noderunner_test

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// registryEntry is one ported it() and its outcome, keyed to the
// upstream spec file and describe block it was ported from.
type registryEntry struct {
	File     string `json:"file"`     // upstream path, e.g. test/nodes/core/common/20-inject_spec.js
	Describe string `json:"describe"` // verbatim upstream describe() title
	Title    string `json:"title"`    // verbatim upstream it() title
	Status   string `json:"status"`   // covered | skipped | failed
	Reason   string `json:"reason,omitempty"`
}

var (
	registryMu sync.Mutex
	registry   []registryEntry
)

func record(e registryEntry) {
	registryMu.Lock()
	registry = append(registry, e)
	registryMu.Unlock()
}

var sanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// sanitizeName maps a verbatim upstream title to a valid Go subtest
// name: non-alphanumeric runs collapse to "_", edges trimmed.
func sanitizeName(title string) string {
	return strings.Trim(sanitizeRe.ReplaceAllString(title, "_"), "_")
}

// suite binds a set of ported it() subtests to one upstream describe
// block in one upstream spec file.
type suite struct {
	t        *testing.T
	file     string
	describe string
}

// describe opens a subtest for the upstream describe() block of the
// given upstream spec file (path as it appears in the node-red repo,
// e.g. "test/nodes/core/function/10-switch_spec.js").
func describe(t *testing.T, file, title string, fn func(s *suite)) {
	t.Helper()
	s := &suite{file: file, describe: title}
	t.Run(sanitizeName(title), func(t *testing.T) {
		s.t = t
		fn(s)
	})
}

// it registers one ported assertion. title MUST be the verbatim
// upstream it() string — it is the join key for the coverage diff.
// A deliberate non-goal is expressed as t.Skip("reason: ...") inside
// fn; the subtest still registers its title (status "skipped") so the
// diff counts the scoping decision.
func (s *suite) it(title string, fn func(t *testing.T)) {
	s.t.Helper()
	s.t.Run(sanitizeName(title), func(t *testing.T) {
		defer func() {
			status := "covered"
			switch {
			case t.Skipped():
				status = "skipped"
			case t.Failed():
				status = "failed"
			}
			record(registryEntry{
				File:     s.file,
				Describe: s.describe,
				Title:    title,
				Status:   status,
			})
		}()
		fn(t)
	})
}

// -------------------------------------------------------------------
// Flow helpers: deploy a workflow, drive it, observe it.
// -------------------------------------------------------------------

// deployFlow deploys wf and fails the test on error; the workflow is
// stopped on test cleanup.
func specDeployFlow(t *testing.T, wf *Workflow) *Engine {
	t.Helper()
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(wf.ID) })
	return engine
}

// injectInto sends payload downstream of fromNodeID and returns the
// response channel (resolved by an httpResponse node).
func specInjectInto(t *testing.T, engine *Engine, wfID, fromNodeID string, p nodestore.MessageInterface) chan nodestore.MessageInterface {
	t.Helper()
	_, ch, err := engine.InjectMessage(wfID, fromNodeID, p)
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	return ch
}

// awaitResponse waits for the pending channel to resolve.
func specAwaitResponse(t *testing.T, ch chan nodestore.MessageInterface, timeout time.Duration) nodestore.MessageInterface {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(timeout):
		t.Fatalf("no response within %s", timeout)
		return nil
	}
}

// awaitEvent polls the workflow event log until an event's message
// satisfies want, or fails after timeout.
func specAwaitEvent(t *testing.T, engine *Engine, wfID string, want func(NodeEvent) bool, timeout time.Duration) NodeEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(wfID, 0)
		for _, ev := range events {
			if want(ev) {
				return ev
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(wfID, 0)
	b, _ := json.Marshal(events)
	t.Fatalf("expected event never appeared; events=%s", b)
	return NodeEvent{}
}

// awaitStatus polls until nodeID reaches the wanted status.
func specAwaitStatus(t *testing.T, engine *Engine, wfID, nodeID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(wfID, 0)
		if nodes[nodeID] == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, nodes, _, _ := engine.GetWorkflowSnapshot(wfID, 0)
	t.Fatalf("node %s never reached status %q; statuses=%v", nodeID, want, nodes)
}

// equalValue compares two payload values structurally (JSON-like data).
func equalValue(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

// debugNode is a workflow node definition of a debug sink.
func specDebugNode(id string) Node {
	return Node{ID: id, Type: "debug", Data: map[string]interface{}{}}
}

// responseNode is a terminal httpResponse node (resolves the pending
// channel of InjectMessage).
func specResponseNode(id string) Node {
	return Node{ID: id, Type: "httpResponse", Data: map[string]interface{}{}}
}
