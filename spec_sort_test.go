package noderunner_test

import (
	"context"
	"encoding/json"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestSortSpec ports assertions from
// test/nodes/core/sequence/18-sort_spec.js (upstream describe
// "SORT node"). Element sorts drive the executor directly; sequence
// sorts deploy src → sort → debug and observe the re-emitted order in
// the event log (the engine re-emits sorted messages asynchronously).
func TestSortSpec(t *testing.T) {
	describe(t, "test/nodes/core/sequence/18-sort_spec.js", "SORT node", func(s *suite) {

		// elemSort mirrors check_sort0A: sort a payload array in place.
		elemSort := func(t *testing.T, order string, asNum bool, in, want []interface{}) {
			t.Helper()
			n := NewSortNode("n1", &SortConfig{
				Order: order, AsNum: asNum,
			}, nil, "")
			data := append([]interface{}(nil), in...)
			out, err := n.Execute(context.Background(), &Message{
				Payload: data, Timestamp: time.Now(),
			})
			if err != nil || out == nil {
				t.Fatalf("execute failed: out=%v err=%v", out, err)
			}
			arr, ok := out.GetPayload().([]interface{})
			if !ok || len(arr) != len(want) {
				t.Fatalf("expected %v, got %v", want, out.GetPayload())
			}
			for i := range want {
				if !equalValue(arr[i], want[i]) {
					t.Fatalf("index %d: expected %v, got %v (full: %v)", i, want[i], arr[i], arr)
				}
			}
		}

		// seqSort deploys a sort node over a 4-message parts sequence
		// and returns the sorted Data values as observed downstream,
		// plus each message's parts metadata (re-indexed upstream).
		seqSort := func(t *testing.T, id string, order string, asNum bool, seqKey string, prop string, in []interface{}) ([]interface{}, []map[string]interface{}) {
			t.Helper()
			wf := &Workflow{
				ID: id,
				Nodes: []Node{
					{ID: "src", Type: "inject", Data: map[string]interface{}{}},
					{ID: "st", Type: "sort", Data: map[string]interface{}{
						// nodebase uses the Node-RED serialized spelling
						// as_num; seqKey is a full message path, so payload
						// object fields are addressed as payload.<field>.
						"target": "seq", "order": order, "as_num": asNum, "seqKey": seqKey,
					}},
					{ID: "dbg", Type: "debug", Data: map[string]interface{}{"complete": "true"}},
				},
				Edges: []Edge{
					{ID: "e1", Source: "src", Target: "st"},
					{ID: "e2", Source: "st", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			for i, v := range in {
				data := interface{}(v)
				if prop != "payload" {
					data = map[string]interface{}{prop: v}
				}
				p := &Message{
					Payload: data,
					Metadata: map[string]interface{}{
						"parts": map[string]interface{}{"id": "X", "index": i, "count": len(in)},
					},
					Timestamp: time.Now(),
				}
				msgID, _, err := engine.InjectMessage(wf.ID, "src", p)
				if err != nil {
					t.Fatalf("inject failed: %v", err)
				}
				engine.DiscardPending(msgID)
			}
			// wait for len(in) debug events from dbg
			deadline := time.Now().Add(3 * time.Second)
			var events []NodeEvent
			for time.Now().Before(deadline) {
				_, _, all, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
				events = events[:0]
				for _, ev := range all {
					if ev.NodeID == "dbg" && ev.Level == "debug" {
						events = append(events, ev)
					}
				}
				if len(events) >= len(in) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if len(events) < len(in) {
				t.Fatalf("expected %d messages downstream, got %d", len(in), len(events))
			}
			var values []interface{}
			var partsList []map[string]interface{}
			for _, ev := range events[:len(in)] {
				var payload struct {
					Payload  interface{}            `json:"payload"`
					Metadata map[string]interface{} `json:"metadata"`
				}
				if err := json.Unmarshal([]byte(ev.Message), &payload); err != nil {
					t.Fatalf("debug event not JSON: %v (%q)", err, ev.Message)
				}
				if prop != "payload" {
					if m, ok := payload.Payload.(map[string]interface{}); ok {
						values = append(values, m[prop])
					} else {
						t.Fatalf("expected object payload, got %v", payload.Payload)
					}
				} else {
					values = append(values, payload.Payload)
				}
				if parts, ok := payload.Metadata["parts"].(map[string]interface{}); ok {
					partsList = append(partsList, parts)
				}
			}
			return values, partsList
		}

		// checkSeq asserts sorted values and upstream re-indexed parts.
		checkSeq := func(t *testing.T, order string, asNum bool, seqKey, prop string, in, want []interface{}) {
			t.Helper()
			values, partsList := seqSort(t, "spec-sort-"+sanitizeName(t.Name()), order, asNum, seqKey, prop, in)
			for i := range want {
				if !equalValue(values[i], want[i]) {
					t.Fatalf("position %d: expected %v, got %v (full: %v)", i, want[i], values[i], values)
				}
			}
			if len(partsList) != len(want) {
				t.Fatalf("missing parts metadata: %v", partsList)
			}
			for i, parts := range partsList {
				if idx, _ := parts["index"].(float64); int(idx) != i {
					t.Fatalf("position %d: parts.index=%v (not re-indexed)", i, parts["index"])
				}
				if cnt, _ := parts["count"].(float64); int(cnt) != len(want) {
					t.Fatalf("position %d: parts.count=%v, expected %d", i, parts["count"], len(want))
				}
			}
		}

		s.it("should be loaded", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-sort-loaded",
				Nodes: []Node{
					{ID: "n1", Type: "sort", Data: map[string]interface{}{}},
					specDebugNode("n2"),
				},
				Edges: []Edge{{ID: "e1", Source: "n1", Target: "n2"}},
			}
			engine := specDeployFlow(t, wf)
			if _, ok := engine.LookupNode(wf.ID, "n1"); !ok {
				t.Fatal("sort node not loaded")
			}
		})

		inStrings := []interface{}{"200", "4", "30", "1000"}

		s.it("should sort payload (elem, not number, ascending)", func(t *testing.T) {
			elemSort(t, "ascending", false, inStrings, []interface{}{"1000", "200", "30", "4"})
		})
		s.it("should sort msg prop (elem, not number, ascending)", func(t *testing.T) {
			skipf(t, "sorting an arbitrary msg property (target=msg prop) is not supported — only payload")
		})
		s.it("should sort message group/payload (not number, ascending)", func(t *testing.T) {
			checkSeq(t, "ascending", false, "payload", "payload", inStrings, []interface{}{"1000", "200", "30", "4"})
		})
		s.it("should sort message group/prop (not number, ascending)", func(t *testing.T) {
			checkSeq(t, "ascending", false, "payload.data", "data", inStrings, []interface{}{"1000", "200", "30", "4"})
		})

		s.it("should sort payload (elem, not number, descending)", func(t *testing.T) {
			elemSort(t, "descending", false, inStrings, []interface{}{"4", "30", "200", "1000"})
		})
		s.it("should sort msg prop (elem, not number, descending)", func(t *testing.T) {
			skipf(t, "sorting an arbitrary msg property (target=msg prop) is not supported — only payload")
		})
		s.it("should sort message group/payload (not number, descending)", func(t *testing.T) {
			checkSeq(t, "descending", false, "payload", "payload", inStrings, []interface{}{"4", "30", "200", "1000"})
		})
		s.it("should sort message group/prop (not number, descending)", func(t *testing.T) {
			checkSeq(t, "descending", false, "payload.data", "data", inStrings, []interface{}{"4", "30", "200", "1000"})
		})

		s.it("should sort payload (elem, number, ascending)", func(t *testing.T) {
			elemSort(t, "ascending", true, inStrings, []interface{}{"4", "30", "200", "1000"})
		})
		s.it("should sort msg prop (elem, number, ascending)", func(t *testing.T) {
			skipf(t, "sorting an arbitrary msg property (target=msg prop) is not supported — only payload")
		})
		s.it("should sort message group/payload (number, ascending)", func(t *testing.T) {
			checkSeq(t, "ascending", true, "payload", "payload", inStrings, []interface{}{"4", "30", "200", "1000"})
		})
		s.it("should sort message group/prop (number, ascending)", func(t *testing.T) {
			checkSeq(t, "ascending", true, "payload.data", "data", inStrings, []interface{}{"4", "30", "200", "1000"})
		})

		s.it("should sort payload (elem, number, descending)", func(t *testing.T) {
			elemSort(t, "descending", true, inStrings, []interface{}{"1000", "200", "30", "4"})
		})
		s.it("should sort msg prop (elem, number, descending)", func(t *testing.T) {
			skipf(t, "sorting an arbitrary msg property (target=msg prop) is not supported — only payload")
		})
		s.it("should sort message group/payload (number, descending)", func(t *testing.T) {
			checkSeq(t, "descending", true, "payload", "payload", inStrings, []interface{}{"1000", "200", "30", "4"})
		})
		s.it("should sort message group/prop (number, descending)", func(t *testing.T) {
			checkSeq(t, "descending", true, "payload.data", "data", inStrings, []interface{}{"1000", "200", "30", "4"})
		})

		s.it("should sort payload (exp, not number, ascending)", func(t *testing.T) {
			skipf(t, "JSONata sort expressions not supported")
		})
		s.it("should sort message group (exp, not number, ascending)", func(t *testing.T) {
			skipf(t, "JSONata sort expressions not supported")
		})
		s.it("should sort message group (exp, not number, descending)", func(t *testing.T) {
			skipf(t, "JSONata sort expressions not supported")
		})
		s.it("should sort payload (exp, not number, descending)", func(t *testing.T) {
			skipf(t, "JSONata sort expressions not supported")
		})

		s.it("should sort payload of objects", func(t *testing.T) {
			skipf(t, "sorting object arrays by a JSONata key expression not supported")
		})

		s.it("should sort payload by context (exp, not number, ascending)", func(t *testing.T) {
			skipf(t, "JSONata + flow context expressions not supported")
		})
		s.it("should sort message group by context (exp, not number, ascending)", func(t *testing.T) {
			skipf(t, "JSONata + flow context expressions not supported")
		})
		s.it("should sort payload by persistable context (exp, not number, descending)", func(t *testing.T) {
			skipf(t, "JSONata + persistable context not supported")
		})
		s.it("should sort message group by persistable context (exp, not number, descending)", func(t *testing.T) {
			skipf(t, "JSONata + persistable context not supported")
		})

		s.it("should handle JSONata script error", func(t *testing.T) {
			skipf(t, "JSONata expressions not supported")
		})

		s.it("should handle too many pending messages", func(t *testing.T) {
			skipf(t, "nodeMessageBufferMaxLength pending-buffer cap not implemented")
		})

		s.it("should clear pending messages on close", func(t *testing.T) {
			skipf(t, "no on-close pending-buffer notification equivalent")
		})

		s.it("should call done() when message is sent (payload)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() when message is sent (sequence)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() regardless of buffer overflow (same group)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
		s.it("should call done() regardless of buffer overflow (different group)", func(t *testing.T) {
			skipf(t, "Node-RED send/done messaging API has no equivalent in this engine")
		})
	})
}
