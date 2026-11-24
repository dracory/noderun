package noderunner_test

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	function "github.com/dracory/nodebase/function"
	"testing"
	"time"
)

// TestMessageEnvelopeFanOutIdentity covers the envelope contract end to
// end: inject births one message instance (UUIDv7 ID + lineage msgId),
// each fan-out edge clone carries its own fresh ID and records the
// birth instance as metadata.parentMsgId.
func TestMessageEnvelopeFanOutIdentity(t *testing.T) {
	engine := NewEngine()
	workflow := &Workflow{
		ID:   "wf-envelope-fanout",
		Name: "envelope fanout",
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "d1", Type: "debug", Data: map[string]interface{}{"complete": "true"}},
			{ID: "d2", Type: "debug", Data: map[string]interface{}{"complete": "true"}},
			{ID: "d3", Type: "debug", Data: map[string]interface{}{"complete": "true"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "d1"},
			{ID: "e2", Source: "inj", Target: "d2"},
			{ID: "e3", Source: "inj", Target: "d3"},
		},
	}
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	birth := &Message{Payload: "x"}
	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", birth)
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)
	if birth.ID == "" {
		t.Fatal("inject must stamp a birth envelope ID")
	}

	events := waitForEvents(engine, workflow.ID, 3*time.Second, func(ev NodeEvent) bool {
		return ev.NodeID == "d3"
	})
	type captured struct{ id, parent, lineage string }
	got := map[string]captured{}
	for _, ev := range events {
		if ev.NodeID == "" {
			continue
		}
		var m struct {
			ID       string                 `json:"id"`
			Metadata map[string]interface{} `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(ev.Message), &m); err != nil || m.ID == "" {
			continue
		}
		parent, _ := m.Metadata["parentMsgId"].(string)
		lineage, _ := m.Metadata["msgId"].(string)
		got[ev.NodeID] = captured{id: m.ID, parent: parent, lineage: lineage}
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 fan-out messages, got %d (%v)", len(got), events)
	}
	ids := map[string]bool{}
	for nodeID, c := range got {
		if ids[c.id] {
			t.Fatalf("duplicate envelope ID %q on %s", c.id, nodeID)
		}
		ids[c.id] = true
		if c.id == birth.ID {
			t.Fatalf("%s kept the birth ID — a fan-out clone must be a new instance", nodeID)
		}
		if c.parent != birth.ID {
			t.Fatalf("%s parentMsgId = %q, want birth ID %q", nodeID, c.parent, birth.ID)
		}
		if c.lineage != msgID {
			t.Fatalf("%s msgId = %q, want shared lineage %q", nodeID, c.lineage, msgID)
		}
	}
}

// TestFunctionNodeMsgIdMapping covers the Node-RED _msgid round-trip:
// the incoming message exposes msg._msgid, a pass-through keeps the ID,
// and a fresh node.send output gets a new ID with parentMsgId set.
func TestFunctionNodeMsgIdMapping(t *testing.T) {
	run := func(t *testing.T, script, inID string) nodestore.MessageInterface {
		t.Helper()
		engine := NewEngine()
		n := function.NewFunctionNode(nodestore.NewNode("fn", "function").SetWorkflowID("sandbox-wf"), &function.FunctionConfig{Func: script}, engine)
		out, err := n.Execute(context.Background(), &Message{
			ID:        inID,
			Payload:   map[string]interface{}{},
			Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatalf("script %q failed: %v", script, err)
		}
		return out
	}

	// _msgid exposure: the script reads the incoming envelope ID.
	out := run(t, "return {payload: msg._msgid};", "in-1")
	if out.GetPayload() != "in-1" {
		t.Fatalf("msg._msgid = %v, want in-1", out.GetPayload())
	}
	// A brand-new object is a new instance: fresh ID + parentage.
	if out.GetID() == "" || out.GetID() == "in-1" {
		t.Fatalf("fresh send must get a new ID, got %q", out.GetID())
	}
	if p, _ := out.GetMetadata()["parentMsgId"].(string); p != "in-1" {
		t.Fatalf("parentMsgId = %q, want in-1", p)
	}

	// Pass-through keeps the same instance ID (Node-RED identity).
	out = run(t, "return msg;", "in-2")
	if out.GetID() != "in-2" {
		t.Fatalf("pass-through ID = %q, want in-2", out.GetID())
	}

	// node.send of a fresh object also gets a new ID + parentage.
	out = run(t, "node.send({payload: 1}); return null;", "in-3")
	if out.GetID() == "" || out.GetID() == "in-3" {
		t.Fatalf("node.send output must get a new ID, got %q", out.GetID())
	}
	if p, _ := out.GetMetadata()["parentMsgId"].(string); p != "in-3" {
		t.Fatalf("parentMsgId = %q, want in-3", p)
	}
}
