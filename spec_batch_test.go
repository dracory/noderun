package noderunner_test

import (
	. "github.com/dracory/noderun"
	"testing"
)

// TestBatchSpec ports assertions from
// test/nodes/core/sequence/19-batch_spec.js (upstream describe
// "BATCH node"). NOTE: upstream's batch node re-emits buffered
// messages as a parts-tagged SEQUENCE (one message per input with
// re-numbered parts.index/count); the engine's batch node emits a single
// message whose Data is an array of the batched payloads. That is a
// fundamental output-shape divergence, so the per-mode titles are
// registered as skipped with the reason documented — the coverage
// report records the scoping decision rather than faking parity.
func TestBatchSpec(t *testing.T) {
	describe(t, "test/nodes/core/sequence/19-batch_spec.js", "BATCH node", func(s *suite) {

		s.it("should be loaded with defaults", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-batch-defaults",
				Nodes: []Node{
					{ID: "n1", Type: "batch", Data: map[string]interface{}{"name": "BatchNode"}},
					specDebugNode("n2"),
				},
				Edges: []Edge{{ID: "e1", Source: "n1", Target: "n2"}},
			}
			engine := specDeployFlow(t, wf)
			n, ok := engine.LookupNode(wf.ID, "n1")
			if !ok || n.GetType() != "batch" {
				t.Fatalf("batch node not loaded: %v", n)
			}
			if name, _ := n.GetData()["name"].(string); name != "BatchNode" {
				t.Fatalf("expected name BatchNode, got %q", name)
			}
		})

		// --- describe('mode: count') -----------------------------------
		// All count-mode titles assert a re-emitted parts SEQUENCE;
		// the engine emits one array message instead.
		s.it("should create seq. with count", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with count (more sent than count)", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with count and terminate early if parts honoured", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with count and overlap", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should handle too many pending messages", func(t *testing.T) {
			skipf(t, "nodeMessageBufferMaxLength pending-buffer cap not implemented")
		})
		s.it("should handle reset", func(t *testing.T) {
			skipf(t, "msg.reset buffer flush not implemented in batch node")
		})

		// --- describe('mode: interval') --------------------------------
		s.it("should create seq. with interval", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with interval (in float)", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with interval & not send empty seq", func(t *testing.T) {
			skipf(t, "the engine emits a single array message, not a parts sequence")
		})
		s.it("should create seq. with interval & send empty seq", func(t *testing.T) {
			skipf(t, "upstream emits an empty sequence marker; the engine emits a single array message")
		})
		s.it("should handle too many pending messages", func(t *testing.T) {
			skipf(t, "nodeMessageBufferMaxLength pending-buffer cap not implemented")
		})
		s.it("should handle reset", func(t *testing.T) {
			skipf(t, "msg.reset buffer flush not implemented in batch node")
		})

		// --- describe('mode: concat') ----------------------------------
		s.it("should concat two seq. (series)", func(t *testing.T) {
			skipf(t, "upstream concat emits one message per input seq element; the engine emits a single flattened array message")
		})
		s.it("should concat two seq. (mixed)", func(t *testing.T) {
			skipf(t, "upstream concat emits one message per input seq element; the engine emits a single flattened array message")
		})
		s.it("should concat three seq.", func(t *testing.T) {
			skipf(t, "upstream concat emits one message per input seq element; the engine emits a single flattened array message")
		})
		s.it("should concat same seq.", func(t *testing.T) {
			skipf(t, "upstream concat emits one message per input seq element; the engine emits a single flattened array message")
		})
		s.it("should handle too many pending messages", func(t *testing.T) {
			skipf(t, "nodeMessageBufferMaxLength pending-buffer cap not implemented")
		})
		s.it("should handle reset", func(t *testing.T) {
			skipf(t, "msg.reset buffer flush not implemented in batch node")
		})

		// --- describe('messaging API') ---------------------------------
		s.it("should call done() when message is sent (mode: count)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when reset (mode: count)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() regardless of buffer overflow (mode: count)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when message is sent (mode: interval)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when reset (mode: interval)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() regardless of buffer overflow (mode: interval)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when message is sent (mode: concat)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when reset (mode: concat)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() regardless of buffer overflow (mode: concat)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
	})
}
