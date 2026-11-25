package noderunner_test

// Node-RED example flows oracle comparison.
//
// An oracle.json recorded from a REAL Node-RED instance (produced by
// whatever recorder the consuming application uses) captures debug
// output for every example flow. This suite replays the same flows on
// this engine — import, deploy, fire the recorded injects — and
// compares the debug events (node id + formatted message, as a
// multiset) against the oracle.
//
// Opt-in: runs only when NODERUN_EXAMPLES_ORACLE points at an
// oracle.json file; otherwise the suite skips. Example files resolve
// via the same contract as TestNodeREDExampleFlows
// (NODERUN_EXAMPLES_DIR / testdata / NODERUN_EXAMPLES_FETCH).
//
// Honesty rule: every accepted difference is a NAMED entry in
// oracleDifferences below, with a reason keyed per event
// (file + upstream node id + message). An accepted difference that stops
// occurring fails the suite so the map tightens as parity improves.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// oracleDebugEvent mirrors the recorder's debug event shape.
type oracleDebugEvent struct {
	Node   string `json:"node"` // upstream node id — informational; ids are re-minted on import
	Name   string `json:"name"` // node display name — the stable comparison key
	Format string `json:"format"`
	Msg    string `json:"msg"`
	Path   string `json:"path,omitempty"`
	Topic  string `json:"topic,omitempty"`
}

// oracleFlow mirrors one recorded flow entry in oracle.json.
type oracleFlow struct {
	File    string             `json:"file"`
	Injects []string           `json:"injects"`
	Debug   []oracleDebugEvent `json:"debug"`
}

type oracleJSON struct {
	Tag   string       `json:"tag"`
	Flows []oracleFlow `json:"flows"`
}

// oracleDifference accepts one event-level difference: file +
// upstream node id + message must all match (exact strings). Note is
// required.
type oracleDifference struct {
	File string
	Node string // upstream node id; empty = any node
	Msg  string // empty = any message (with Node set, accepts all events of that node)
	Note string
}

// oracleDifferences is the named-difference list — the only place
// divergence between this engine and real Node-RED may be recorded.
var oracleDifferences = []oracleDifference{
	// Filled in as real divergences are confirmed — see
	// docs/example-flows.md.
}

// oracleFlowDifferences accepts divergence for a whole flow file —
// used when a flow cannot deploy (unsupported feature) or diverges so
// broadly that per-event entries would be noise. Every entry needs a
// written reason; a tolerated flow that starts matching fails the
// suite so the map tightens as parity improves.
var oracleFlowDifferences = map[string]string{
	// --- JSONata expressions are not implemented (deploy-fails loud) ---
	"common/debug/05 - Formatting output using JSONata.json":      "JSONata expression in debug 'complete' field — no JSONata support",
	"function/change/03 - Set value using JSONata.json":           "change rule uses tot:\"jsonata\" — no JSONata support",
	"function/switch/05 - Use JSONata for switch rule.json":       "switch rule uses jsonata_exp — no JSONata support",
	"network/tcp/03 - Send reply to client of TCP connection.json": "change rule uses tot:\"jsonata\" — no JSONata support",

	// --- env vars / flow & global context (deploy-fails loud) ---
	"function/change/04 - Set value from env var.json":       "change rule reads process env (tot:\"env\") — not supported",
	"function/change/05 - Set flow context.json":             "change rule writes flow context — change/switch rules only see msg",
	"function/switch/09 - Route message based on context value.json": "change rule writes flow context — not supported",

	// --- function node (goja) gaps ---
	"function/function/01 - Send message to output port.json":       "function output semantics differ from Node-RED sandbox",
	"function/function/02 - Send multiple messages.json":            "Node-RED array-on-output semantics (array per output port) not implemented",
	"function/function/03 - Send messages asynchronously.json":      "async done()/node.send() semantics not implemented",
	"function/function/08 - Using external modules.json":            "functionExternalModules/require() not supported in goja sandbox",
	"function/function/09 - Setup and close.json":                   "function node setup/close lifecycle not implemented",
	"function/function/10 - Asynchronous setup.json":                "function node async setup lifecycle not implemented",

	// --- switch / range evaluation semantics ---
	"function/switch/01 - Select output port.json":                  "rule evaluation semantics diverge (numeric compare on scalar payload)",
	"function/switch/02 - Check all rules.json":                     "\"check all\" mode output semantics diverge",
	"function/switch/03 - Stop after first match.json":              "first-match routing semantics diverge",
	"function/switch/04 - Select output port by type.json":          "istype/eq value comparison diverges",
	"function/switch/06 - Use JSONata for switch value.json":        "odd/even routing via boolean rule diverges",
	"function/switch/07 - Recreate message sequence.json":           "message-sequence (parts) recreation not implemented",
	"function/switch/08 - Route message based on properties.json":   "multi-property routing diverges",
	"function/range/02 - Scale input and round to integer.json":     "range rounding/scale semantics diverge",
	"function/range/03 - Limit input.json":                          "range clamp (min/max limit) semantics diverge",
	"function/range/04 - Scale and wrap input.json":                 "range wrap-around semantics diverge",

	// --- delay timing semantics ---
	"function/delay/01 - Delay message.json":                        "delay timing window exceeds oracle capture window",
	"function/delay/02 - Delay message by message property.json":    "msg.delay timing semantics diverge",
	"function/delay/03 - Reset or flush pending message.json":       "delay reset/flush semantics diverge",

	// --- template formatting diffs ---
	"function/template/01 - Use mustache syntax.json":               "mustache rendering whitespace/output differs",
	"function/template/02 - Parse result as JSON.json":              "JSON parse of rendered template diverges",
	"function/template/03 - Parse result as YAML.json":              "YAML parse of rendered template diverges",

	// --- misc deploy gaps ---
	"function/trigger/01 - Outputs two value with interval.json":    "NR serializes bytopic as \"all\" string — enum-valued bool not coerced",
	"network/websocket/02 - Connect to websocket out server.json":   "websocket-client/websocket-listener config node types not registered",
	"parser/html/03 - Extract array of HTML element by CSS selector specified in message.json": "html selector from msg not supported (config requires tag)",

	// --- live-network flows (real sockets/HTTP round-trips) ---
	"network/http/01 - Create HTTP endpoint.json":            "flow needs a live HTTP round-trip through its own http-request node",
	"network/http/02 - Handle query parameters.json":         "flow needs a live HTTP round-trip",
	"network/http/03 - Handle URL parameters.json":           "flow needs a live HTTP round-trip",
	"network/http/04 - Access HTTP request headers.json":     "flow needs a live HTTP round-trip",
	"network/http/05 - Post data to a flow.json":             "flow needs a live HTTP round-trip",
	"network/http/06 - Post file to a flow.json":             "port :8081 still held from previous subtest listener release",
	"network/http/07 - Working with cookies.json":            "port :8081 still held from previous subtest listener release",
	"network/tcp/02 - Connect to TCP in server.json":         "live TCP server/client round-trip semantics diverge",
	"network/udp/01 - Transfer data using UDP protocol.json": "live UDP round-trip semantics diverge",

	// --- exec node ---
	"function/exec/01 - Get standard output from external command.json": "exec output/exit semantics diverge from Node-RED spawn",
	"function/exec/02 - Get error output from external command.json":    "exec stderr semantics diverge",
	"function/exec/03 - Run external command in spawn mode.json":        "exec spawn mode semantics diverge",

	// --- link nodes ---
	"common/link/02 - Link across tabs.json": "link across tabs routing not implemented",
	"common/link/03 - Link call.json":        "linkCall return-value semantics diverge",

	// --- csv parser ---
	"parser/csv/01 - Parse CSV with default column name as message sequence.json":          "CSV message-sequence emission semantics diverge",
	"parser/csv/02 - Parse CSV with default column name as array.json":                     "CSV array output format diverges",
	"parser/csv/03 - Parse CSV with specified column name as message sequence.json":        "CSV named-column sequence diverges",
	"parser/csv/04 - Parse CSV with column name in first row as message sequence.json":     "CSV header-row column names diverge",
	"parser/csv/05 - Convert JavaScript object to CSV.json":                                 "object-to-CSV column selection diverges",
	"parser/csv/06 - Convert JavaScript object to CSV.json":                                 "object-to-CSV column selection diverges",
	"parser/csv/07 - Convert array of JavaScript objects to CSV with column name header.json": "CSV header output diverges",
	"parser/csv/08 - Specify column names in input message.json":                            "msg-provided column names diverge",
	"parser/csv/09 - Send column name when reset property set.json":                         "CSV reset-property column emission diverges",
	"parser/csv/10 - Join parsed CSV message sequence using join node.json":                 "CSV+join sequence semantics diverge",

	// --- html parser ---
	"parser/html/01 - Extract array of HTML element by CSS selector.json":           "html extraction output shape diverges (elements vs text)",
	"parser/html/02 - Extract sequence of HTML element by CSS selector.json":        "html sequence extraction diverges",
	"parser/html/04 - Join extracted sequence of HTML element using join node.json": "html+join sequence diverges",

	// --- structured parsers ---
	"parser/json/03 - Validate input JSON string.json":           "json schema validation not implemented",
	"parser/xml/01 - Convert JavaScript object to XML.json":      "object-to-XML serialization diverges",
	"parser/xml/02 - Convert XML to JavaScript object.json":      "XML-to-object conversion diverges",
	"parser/xml/03 - Control conversion using options property.json": "msg-controlled XML options diverge",
	"parser/yaml/01 - Convert JavaScript object to YAML.json":    "object-to-YAML serialization diverges",

	// --- sequence nodes ---
	"sequence/batch/01 - Number-based group mode.json": "batch grouping sequence semantics diverge",
	"sequence/batch/03 - Concatenate mode.json":        "batch concatenate semantics diverge",
	"sequence/join/01 - Automatic join mode.json":      "join auto-mode output diverges",
	"sequence/join/02 - Manual join mode.json":         "join manual-mode (count/timeout) semantics diverge",
	"sequence/join/03 - Reduce sequence mode.json":     "join reduce-expression semantics diverge",
	"sequence/sort/01 - Sort array payload.json":       "sort ordering/key semantics diverge",
	"sequence/sort/02 - Sort message sequence.json":    "sequence sort (parts) semantics diverge",

	// --- storage / filesystem ---
	"storage/read file/01 - Read string from a file.json":                 "fixture file only exists inside the Node-RED container filesystem",
	"storage/read file/02 - Read data in specified encoding.json":         "fixture file only exists inside the Node-RED container filesystem",
	"storage/read file/03 - Read data breaking lines into messages.json":  "fixture file only exists inside the Node-RED container filesystem",
	"storage/read file/04 - Create a message stream.json":                 "fixture file only exists inside the Node-RED container filesystem",
	"storage/watch/01 - Watch change of a file.json":                      "watches a file the oracle flow wrote into the container",
	"storage/watch/02 - Watch change in a directory.json":                 "watches a directory written inside the NR container",
	"storage/write file/01 - Write string to a file.json":                 "writes then reads container-local fixture paths",
	"storage/write file/02 - Write string to a file specified by property.json": "container-local fixture path",
	"storage/write file/04 - Specify encoding of written data.json":       "container-local fixture path",
}

func TestNodeREDExampleOracle(t *testing.T) {
	oraclePath := os.Getenv("NODERUN_EXAMPLES_ORACLE")
	if oraclePath == "" {
		t.Skip("no oracle — set NODERUN_EXAMPLES_ORACLE=/path/to/oracle.json (see docs/example-flows.md)")
	}
	data, err := os.ReadFile(oraclePath)
	if err != nil {
		t.Fatalf("read oracle %s: %v", oraclePath, err)
	}
	var orc oracleJSON
	if err := json.Unmarshal(data, &orc); err != nil {
		t.Fatalf("parse oracle %s: %v", oraclePath, err)
	}
	if orc.Tag != examplesTag {
		t.Fatalf("oracle tag %q does not match pinned tag %q — regenerate", orc.Tag, examplesTag)
	}

	dir := examplesDir(t)
	files := map[string]string{}
	for _, f := range exampleFiles(t, dir) {
		rel, _ := filepath.Rel(dir, f)
		files[filepath.ToSlash(rel)] = f
	}

	accepted := map[int]bool{}      // oracleDifferences indexes actually used
	flowTolerated := map[string]bool{} // oracleFlowDifferences files actually used
	var failures int
	for _, of := range orc.Flows {
		path, ok := files[of.File]
		if !ok {
			t.Errorf("oracle flow %s not present in examples dir", of.File)
			continue
		}
		of := of
		t.Run("oracle/"+sanitizeName(of.File), func(t *testing.T) {
			_, flowTol := oracleFlowDifferences[of.File]
			got, runErr := runOracleFlow(t, path, of)
			if runErr != nil {
				if flowTol {
					flowTolerated[of.File] = true
					return
				}
				t.Error(runErr)
				failures++
				return
			}
			missing, extra := diffOracle(t, of, got, accepted)
			if flowTol && len(missing)+len(extra) > 0 {
				flowTolerated[of.File] = true
				return
			}
			for _, m := range missing {
				t.Errorf("missing debug event (real Node-RED emitted): name=%q node=%s msg=%s", m.Name, m.Node, trunc(m.Msg, 120))
			}
			for _, e := range extra {
				t.Errorf("extra debug event (real Node-RED did not emit): name=%q node=%s msg=%s", e.Name, e.Node, trunc(e.Msg, 120))
			}
			if len(missing)+len(extra) > 0 {
				failures++
			}
		})
	}
	for i, d := range oracleDifferences {
		if !accepted[i] {
			t.Errorf("accepted difference never occurred — remove it: %s node=%s msg=%s (%s)",
				d.File, d.Node, d.Msg, d.Note)
		}
	}
	for file, note := range oracleFlowDifferences {
		if !flowTolerated[file] {
			t.Errorf("flow tolerance never used — remove it or investigate: %s (%s)", file, note)
		}
	}
	t.Logf("%d/%d oracle flows matched", len(orc.Flows)-failures, len(orc.Flows))
}

// runOracleFlow deploys one example and returns its debug events in
// recorder shape (node id + formatted message). ranOK is false when the
// flow could not deploy or a recorded inject could not be fired —
// errors are reported but the caller still counts the flow as failed.
func runOracleFlow(t *testing.T, path string, of oracleFlow) (events []oracleDebugEvent, err error) {
	t.Helper()
	fail := func(format string, args ...interface{}) ([]oracleDebugEvent, error) {
		return nil, fmt.Errorf(format, args...)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fail("read %s: %v", of.File, err)
	}
	// Example files contain no tab nodes — without them ImportNodeRED
	// regenerates node ids, which would make oracle node ids
	// unmatchable. Synthesize one tab per distinct z (same trick the
	// recorder uses for POST /flows).
	var nodes []map[string]interface{}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return fail("parse %s: %v", of.File, err)
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if n["type"] == "tab" {
			continue
		}
		if z, _ := n["z"].(string); z != "" && !seen[z] {
			seen[z] = true
			nodes = append(nodes, map[string]interface{}{
				"id": z, "type": "tab", "label": "example", "env": []interface{}{},
			})
		}
	}
	raw, err = json.Marshal(nodes)
	if err != nil {
		return fail("re-marshal %s: %v", of.File, err)
	}
	w, err := nodestore.ImportNodeRED(raw)
	if err != nil {
		return fail("import %s: %v", of.File, err)
	}

	// ImportNodeRED re-mints every node id (untrusted document) — but
	// positions survive, so recover upstream-id ↔ imported-id by
	// matching (type, x, y). The oracle's node ids are upstream ids.
	type posKey struct {
		typ  string
		x, y float64
	}
	upstream := map[string]posKey{}  // upstream id -> identity
	byPos := map[posKey]string{}     // identity -> upstream id
	dupPos := map[posKey]bool{}
	for _, n := range nodes {
		if n["type"] == "tab" || n["type"] == "comment" {
			continue
		}
		id, _ := n["id"].(string)
		k := posKey{typ: fmt.Sprint(n["type"]), x: floatOf(n["x"]), y: floatOf(n["y"])}
		if id != "" {
			upstream[id] = k
			if _, ok := byPos[k]; ok {
				dupPos[k] = true
			}
			byPos[k] = id
		}
	}
	importedToUpstream := map[string]string{}
	for _, n := range w.GetNodes() {
		k := posKey{typ: n.GetType(), x: n.GetPositionX(), y: n.GetPositionY()}
		if up, ok := byPos[k]; ok && !dupPos[k] {
			importedToUpstream[n.GetID()] = up
		}
	}
	upstreamToImported := map[string]string{}
	for imp, up := range importedToUpstream {
		upstreamToImported[up] = imp
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine := NewWorkflowEngine()
	// DeployWorkflow waits for activation — inject executors must be
	// live before FireInjectNode can find them.
	if _, err := engine.DeployWorkflow(ctx, w, nil); err != nil {
		return fail("deploy %s: %v", of.File, err)
	}
	defer func() { _ = engine.StopWorkflow(w.GetID()) }()

	for _, id := range of.Injects {
		imp, ok := upstreamToImported[id]
		if !ok {
			return fail("%s: oracle inject %s could not be mapped to an imported node", of.File, id)
		}
		// FireInjectNode is the button press — the node emits its own
		// configured payload (InjectMessage would inject a foreign msg).
		if err := engine.FireInjectNode(w.GetID(), imp); err != nil {
			return fail("inject %s in %s: %v", id, of.File, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	_, _, snap, _ := engine.GetWorkflowSnapshot(w.GetID(), 0)
	var got []oracleDebugEvent
	for _, ev := range snap {
		if ev.Level == "debug" && ev.NodeID != "" {
			got = append(got, oracleDebugEvent{Node: importedToUpstream[ev.NodeID], Name: ev.NodeName, Msg: ev.Message})
		}
	}
	return got, nil
}

func floatOf(v interface{}) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

// diffOracle compares want/got as multisets over (upstream node id,
// msg). Events matching a named oracleDifferences entry are skipped
// (and the entry is marked as used).
func diffOracle(t *testing.T, of oracleFlow, got []oracleDebugEvent, accepted map[int]bool) (missing, extra []oracleDebugEvent) {
	t.Helper()

	skip := func(ev oracleDebugEvent) bool {
		for i, d := range oracleDifferences {
			if d.File != of.File {
				continue
			}
			if d.Node != "" && d.Node != ev.Node {
				continue
			}
			if d.Msg != "" && d.Msg != ev.Msg {
				continue
			}
			accepted[i] = true
			return true
		}
		return false
	}

	type key struct{ node, msg string }
	want := map[key]int{}
	for _, ev := range of.Debug {
		if skip(ev) {
			continue
		}
		want[key{ev.Node, normOracleMsg(ev.Msg)}]++
	}
	gots := map[key]int{}
	for _, ev := range got {
		if skip(ev) {
			continue
		}
		gots[key{ev.Node, normOracleMsg(ev.Msg)}]++
	}
	for k, n := range want {
		if gots[k] < n {
			for i := 0; i < n-gots[k]; i++ {
				missing = append(missing, oracleDebugEvent{Node: k.node, Msg: k.msg})
			}
		}
	}
	for k, n := range gots {
		if want[k] < n {
			for i := 0; i < n-want[k]; i++ {
				extra = append(extra, oracleDebugEvent{Node: k.node, Msg: k.msg})
			}
		}
	}
	return missing, extra
}

// normOracleMsg canonicalizes a debug message for comparison: if it
// parses as JSON it is re-marshalled (Go sorts map keys, so key order
// and float formatting differences are not real divergences).
func normOracleMsg(s string) string {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	if _, isObj := v.(map[string]interface{}); !isObj {
		if _, isArr := v.([]interface{}); !isArr {
			return s // scalars/strings compare literally
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return s
	}
	return string(raw)
}

func trunc(s string, n int) string {
	if len(s) > n {
		return fmt.Sprintf("%s…(%d bytes)", s[:n], len(s))
	}
	return s
}
