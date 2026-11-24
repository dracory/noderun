package noderunner

import (
	"context"
	"fmt"
	"net"
	"github.com/dracory/nodebase"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// requireMQTTBroker skips the test when no broker listens on
// localhost:1883 — CI environments typically have none.
func requireMQTTBroker(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:1883", 500*time.Millisecond)
	if err != nil {
		t.Skip("no MQTT broker on localhost:1883 — skipping")
	}
	_ = conn.Close()
}

// TestMQTTConfigAndRegistration verifies config decoding and executor
// construction for both node types without needing a broker.
func TestMQTTConfigAndRegistration(t *testing.T) {
	engine := NewEngine()
	in, err := engine.executorForNode("wf", &Node{ID: "mi", Type: "mqttIn", Data: map[string]interface{}{
		"broker": "localhost:1883", "topic": "test/#", "qos": float64(1), "datatype": "json",
	}})
	if err != nil {
		t.Fatalf("mqttIn executorForNode failed: %v", err)
	}
	if _, ok := in.(Stopper); !ok {
		t.Fatal("mqttIn must implement Stopper")
	}
	out, err := engine.executorForNode("wf", &Node{ID: "mo", Type: "mqttOut", Data: map[string]interface{}{
		"broker": "tcp://localhost:1883", "topic": "test/x", "retain": true,
	}})
	if err != nil {
		t.Fatalf("mqttOut executorForNode failed: %v", err)
	}
	if err := out.Validate(); err != nil {
		t.Fatalf("mqttOut validate failed: %v", err)
	}

	// Validation failure: mqttIn without a topic
	ex, err := engine.executorForNode("wf", &Node{ID: "mi2", Type: "mqttIn", Data: map[string]interface{}{"broker": "b:1"}})
	if err != nil {
		t.Fatalf("mqttIn executorForNode failed: %v", err)
	}
	if verr := ex.Validate(); verr == nil {
		t.Fatal("mqttIn without topic must fail validation")
	}
}

// TestMQTTRoundTrip deploys mqttIn(test/#) -> log and inject -> mqttOut,
// asserting the published message reaches the log. Skipped without a
// broker on localhost:1883.
func TestMQTTRoundTrip(t *testing.T) {
	requireMQTTBroker(t)
	workflow := &Workflow{
		ID:   "test-mqtt-flow",
		Name: "MQTT Flow",
		Nodes: []Node{
			{ID: "min", Type: "mqttIn", Data: map[string]interface{}{"broker": "127.0.0.1:1883", "topic": "goflow/test/#"}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "got"}},
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `"ping"`, "once": true}},
			{ID: "mout", Type: "mqttOut", Data: map[string]interface{}{"broker": "127.0.0.1:1883", "topic": "goflow/test/x"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "min", Target: "lg"},
			{ID: "e2", Source: "inj", Target: "mout"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	nodes := waitStatus(t, engine, workflow.ID, 5*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		t.Fatalf("log never reached ok; statuses=%v", nodes)
	}
}

// TestMQTTSharedSubscriptionValidation checks shareGroup validation
// rules without needing a broker.
func TestMQTTSharedSubscriptionValidation(t *testing.T) {
	engine := NewEngine()
	mk := func(data map[string]interface{}) NodeExecutor {
		ex, err := engine.executorForNode("wf", &Node{ID: "mi", Type: "mqttIn", Data: data})
		if err != nil {
			t.Fatalf("executorForNode failed: %v", err)
		}
		return ex
	}

	if err := mk(map[string]interface{}{
		"broker": "b:1", "topic": "a/b", "shareGroup": "workers",
	}).Validate(); err != nil {
		t.Fatalf("valid shared config rejected: %v", err)
	}
	if err := mk(map[string]interface{}{
		"broker": "b:1", "topic": "a/b", "shareGroup": "bad/group",
	}).Validate(); err == nil {
		t.Fatal("shareGroup containing / must fail validation")
	}
	if err := mk(map[string]interface{}{
		"broker": "b:1", "topic": "$share/g/a/b", "shareGroup": "other",
	}).Validate(); err == nil {
		t.Fatal("shareGroup + raw $share topic must fail validation")
	}
	// Raw $share topic alone remains valid (backwards-compatible passthrough).
	if err := mk(map[string]interface{}{
		"broker": "b:1", "topic": "$share/g/a/b",
	}).Validate(); err != nil {
		t.Fatalf("raw $share topic rejected: %v", err)
	}
}

// TestMQTTSharedSubscriptionDisjoint verifies two broker clients in the
// same share group receive disjoint deliveries — each published message
// reaches exactly one of them. Skipped without a broker on
// localhost:1883.
func TestMQTTSharedSubscriptionDisjoint(t *testing.T) {
	requireMQTTBroker(t)

	const broker = "127.0.0.1:1883"
	const filter = "$share/goflowtest/goflow/shared/t"
	const nMsgs = 20

	var mu sync.Mutex
	received := map[string]int{} // payload -> delivery count across both clients
	done := make(chan struct{}, nMsgs*2)
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		received[string(msg.Payload())]++
		mu.Unlock()
		done <- struct{}{}
	}

	sub := func(id string) mqtt.Client {
		c, err := nodebase.MQTTConnect(broker, id, "", "")
		if err != nil {
			t.Fatalf("connect %s: %v", id, err)
		}
		tok := c.Subscribe(filter, 1, handler)
		if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
			c.Disconnect(250)
			t.Fatalf("subscribe %s: %v", id, tok.Error())
		}
		return c
	}
	c1 := sub("goflow-share-1")
	defer c1.Disconnect(500)
	c2 := sub("goflow-share-2")
	defer c2.Disconnect(500)

	pub, err := nodebase.MQTTConnect(broker, "goflow-share-pub", "", "")
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	defer pub.Disconnect(500)
	for i := 0; i < nMsgs; i++ {
		tok := pub.Publish("goflow/shared/t", 1, false, fmt.Sprintf("m%d", i))
		if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %d: %v", i, tok.Error())
		}
	}

	deadline := time.After(15 * time.Second)
	for i := 0; i < nMsgs; i++ {
		select {
		case <-done:
		case <-deadline:
			mu.Lock()
			t.Fatalf("timed out waiting for deliveries; received=%v", received)
		}
	}
	// Drain briefly for duplicate (non-disjoint) deliveries.
	select {
	case <-done:
		t.Fatal("more deliveries than published messages — shared subscription not disjoint")
	case <-time.After(2 * time.Second):
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != nMsgs {
		t.Fatalf("expected %d distinct payloads, got %d: %v", nMsgs, len(received), received)
	}
	for payload, count := range received {
		if count != 1 {
			t.Fatalf("payload %q delivered %d times — deliveries not disjoint", payload, count)
		}
	}
}
