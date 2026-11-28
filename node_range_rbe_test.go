package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

func rangePayload(data interface{}) *Message {
	return &Message{Payload: data, Timestamp: time.Now()}
}

// TestRangeScale verifies linear mapping and the default payload property.
func TestRangeScale(t *testing.T) {
	n := NewRangeNode("r", &RangeConfig{
		Action: "scale", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 1,
	})
	out, err := n.Execute(context.Background(), rangePayload(float64(50)))
	if err != nil || out == nil {
		t.Fatalf("expected passthrough, got out=%v err=%v", out, err)
	}
	if out.GetPayload() != float64(0.5) {
		t.Fatalf("expected 0.5, got %v", out.GetPayload())
	}
}

// TestRangeClamp verifies clamping to the input range before scaling.
func TestRangeClamp(t *testing.T) {
	n := NewRangeNode("r", &RangeConfig{
		Action: "clamp", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 1,
	})
	out, _ := n.Execute(context.Background(), rangePayload(float64(150)))
	if out == nil || out.GetPayload() != float64(1) {
		t.Fatalf("expected clamped 1, got %v", out)
	}
	out, _ = n.Execute(context.Background(), rangePayload(float64(-50)))
	if out == nil || out.GetPayload() != float64(0) {
		t.Fatalf("expected clamped 0, got %v", out)
	}
}

// TestRangeDrop verifies out-of-range messages are dropped.
func TestRangeDrop(t *testing.T) {
	n := NewRangeNode("r", &RangeConfig{
		Action: "drop", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 1,
	})
	out, err := n.Execute(context.Background(), rangePayload(float64(150)))
	if err != nil || out != nil {
		t.Fatalf("expected drop (nil, nil), got out=%v err=%v", out, err)
	}
	out, _ = n.Execute(context.Background(), rangePayload(float64(50)))
	if out == nil || out.GetPayload() != float64(0.5) {
		t.Fatalf("expected in-range pass, got %v", out)
	}
}

// TestRangeRoll verifies wrapping into the input range before scaling.
func TestRangeRoll(t *testing.T) {
	n := NewRangeNode("r", &RangeConfig{
		Action: "roll", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 100,
	})
	out, _ := n.Execute(context.Background(), rangePayload(float64(150)))
	if out == nil || out.GetPayload() != float64(50) {
		t.Fatalf("expected rolled 50, got %v", out)
	}
	out, _ = n.Execute(context.Background(), rangePayload(float64(-25)))
	if out == nil || out.GetPayload() != float64(75) {
		t.Fatalf("expected rolled 75, got %v", out)
	}
}

// TestRangeNonNumeric verifies non-numeric values are a node error,
// bools and radix-prefixed strings coerce, and a missing property
// passes through.
func TestRangeNonNumeric(t *testing.T) {
	n := NewRangeNode("r", &RangeConfig{
		Action: "scale", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 1,
	})
	out, err := n.Execute(context.Background(), rangePayload("abc"))
	if err == nil || out != nil {
		t.Fatalf("expected non-numeric node error, got out=%v err=%v", out, err)
	}
	out, _ = n.Execute(context.Background(), rangePayload(true))
	if out == nil || out.GetPayload() != float64(0.01) {
		t.Fatalf("expected bool→1→0.01, got %v", out)
	}
	out, _ = n.Execute(context.Background(), rangePayload("0x64"))
	if out == nil || out.GetPayload() != float64(1) {
		t.Fatalf("expected hex 0x64→100→1, got %v", out)
	}

	// Missing property passes through unchanged
	n2 := NewRangeNode("r", &RangeConfig{
		Action: "scale", Property: "missing", MinIn: 0, MaxIn: 1, MinOut: 0, MaxOut: 1,
	})
	in := rangePayload(map[string]interface{}{"other": 1})
	out, _ = n2.Execute(context.Background(), in)
	if out != in {
		t.Fatalf("expected missing-property passthrough, got %v", out)
	}
}

// TestRBEFuncs verifies rbe passes the first message and rbei blocks it,
// both then forwarding only on change (Node-RED semantics: rbei is
// "ignore initial value").
func TestRBEFuncs(t *testing.T) {
	engine := NewEngine()
	ctx := context.Background()

	// rbe: first message passes (any change incl. initial), same
	// blocked, changed passed
	rbe := NewRBENode("n1", &RBEConfig{Func: "rbe"}, engine, "wf")
	if out, _ := rbe.Execute(ctx, rangePayload(float64(1))); out == nil {
		t.Fatalf("rbe: expected first message passed")
	}
	if out, _ := rbe.Execute(ctx, rangePayload(float64(1))); out != nil {
		t.Fatalf("rbe: expected unchanged value dropped, got %v", out)
	}
	if out, _ := rbe.Execute(ctx, rangePayload(float64(2))); out == nil {
		t.Fatalf("rbe: expected changed value passed")
	}

	// rbei: first message blocked, same blocked, changed passed
	rbei := NewRBENode("n2", &RBEConfig{Func: "rbei"}, engine, "wf")
	if out, _ := rbei.Execute(ctx, rangePayload(float64(1))); out != nil {
		t.Fatalf("rbei: expected first message dropped, got %v", out)
	}
	if out, _ := rbei.Execute(ctx, rangePayload(float64(1))); out != nil {
		t.Fatalf("rbei: expected unchanged value dropped, got %v", out)
	}
	if out, _ := rbei.Execute(ctx, rangePayload(float64(2))); out == nil {
		t.Fatalf("rbei: expected changed value passed")
	}
}

// TestRBEDeadband verifies a deadband blocks small moves and passes
// moves beyond the gap.
func TestRBEDeadband(t *testing.T) {
	engine := NewEngine()
	n := NewRBENode("n", &RBEConfig{Func: "deadband", Gap: "0.1"}, engine, "wf")
	ctx := context.Background()

	if out, _ := n.Execute(ctx, rangePayload(float64(1))); out == nil {
		t.Fatalf("expected first message passed")
	}
	if out, _ := n.Execute(ctx, rangePayload(float64(1.05))); out != nil {
		t.Fatalf("expected |0.05| < 0.1 blocked, got %v", out)
	}
	if out, _ := n.Execute(ctx, rangePayload(float64(1.2))); out == nil {
		t.Fatalf("expected |0.2| >= 0.1 passed")
	}
}

// TestRBEReset verifies Metadata["reset"] clears stored state.
func TestRBEReset(t *testing.T) {
	engine := NewEngine()
	n := NewRBENode("n", &RBEConfig{Func: "rbe"}, engine, "wf")
	ctx := context.Background()

	_, _ = n.Execute(ctx, rangePayload(float64(1))) // passes, stores 1
	_, _ = n.Execute(ctx, rangePayload(float64(2))) // passes, stores 2

	// reset clears prev — next message is "first" again → passed (rbe
	// sends on any change, including the initial value)
	rst := rangePayload(float64(3))
	rst.Metadata = map[string]interface{}{"reset": true}
	if out, _ := n.Execute(ctx, rst); out == nil {
		t.Fatalf("expected post-reset first message passed")
	}
	if out, _ := n.Execute(ctx, rangePayload(float64(4))); out == nil {
		t.Fatalf("expected changed value passed after reset")
	}
}

// TestConditionPorts verifies true routes to output_1 and false to
// output_2 via SelectPorts.
func TestConditionPorts(t *testing.T) {
	n := NewConditionNode("c", &ConditionConfig{
		Property: "path", Op: "eq", Value: "/a",
	})
	payload := func(path string) *Message {
		return &Message{
			Payload:   map[string]interface{}{"path": path},
			Timestamp: time.Now(),
		}
	}
	if ports := n.SelectPorts(payload("/a"), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected [output_1], got %v", ports)
	}
	if ports := n.SelectPorts(payload("/b"), nil); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("expected [output_2], got %v", ports)
	}
}

// TestRBENonNumericIsNodeError — BUG-7: band modes error on
// non-numeric input.
func TestRBENonNumericIsNodeError(t *testing.T) {
	engine := NewEngine()
	n := NewRBENode("n", &RBEConfig{Func: "deadband", Gap: "0.1"}, engine, "wf")
	// First message passes (seeds prev)
	if _, err := n.Execute(context.Background(), rangePayload(float64(1))); err != nil {
		t.Fatalf("first message should pass, got %v", err)
	}
	out, err := n.Execute(context.Background(), rangePayload("abc"))
	if err == nil || out != nil {
		t.Fatalf("expected node error, got out=%v err=%v", out, err)
	}
}
