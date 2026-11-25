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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	nodebase "github.com/dracory/nodebase"
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
// upstream node id + normalized message must all match. Note is
// required.
type oracleDifference struct {
	File string
	Node string // upstream node id; empty = any node
	Msg  string // exact event message after normalisation; empty matches only an empty message
	Note string
}

// oracleHTMLH1 is the exact html body the Node-RED example templates
// render for their <h1> greeting pages.
func oracleHTMLH1(text string) string {
	return "<html>\n    <head></head>\n    <body>\n        <h1>" + text + "</h1>\n    </body>\n</html>"
}

// oracleHTTPResponseMsg builds the canonical normalized form of this
// engine's http-request response object. duration and Date are absent
// because normOracleMsg scrubs those volatile fields before comparison.
func oracleHTTPResponseMsg(body, contentLength, contentType, status string, statusCode int, extraHeaders ...map[string][]string) string {
	headers := map[string][]string{
		"Content-Length": {contentLength},
		"Content-Type":   {contentType},
	}
	if len(extraHeaders) > 0 {
		for k, v := range extraHeaders[0] {
			headers[k] = v
		}
	}
	payload := map[string]interface{}{
		"body":          body,
		"bodyTruncated": false,
		"headers":       headers,
		"status":        status,
		"statusCode":    statusCode,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// oracleDifferences is the named-difference list — the only place
// divergence between this engine and real Node-RED may be recorded.
var oracleDifferences = []oracleDifference{
	// --- http: response payload shape residual ---
	// httpRequest emits a response object {body, statusCode, headers,…}
	// while Node-RED puts the response body directly in msg.payload
	// (ret:"txt"). Each residual names both exact sides of the divergence;
	// volatile duration/Date fields are scrubbed before comparison.
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "18b48ed8.1072e1", Msg: oracleHTMLH1("Hello World!"),
		Note: "oracle-side residual: Node-RED places the response body directly in msg.payload"},
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "18b48ed8.1072e1", Msg: oracleHTTPResponseMsg(oracleHTMLH1("Hello World!"), "85", "text/html; charset=utf-8", "200 OK", 200),
		Note: "actual-side residual: httpRequest wraps the correct body in a response object"},
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "188bc18.5487a3f", Msg: oracleHTMLH1("Hello World!"),
		Note: "oracle-side residual: the second request should render the endpoint body; this engine keeps msg.url in metadata so that request never runs"},
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "", Msg: "invalid input data format",
		Note: "actual-side residual: second httpRequest rejects the inject message — msg.url arrives in metadata, not the payload map"},
	{File: "network/http/02 - Handle query parameters.json", Node: "f523b8e.b5c8c48", Msg: oracleHTMLH1("Hello Nick!"),
		Note: "oracle-side residual: Node-RED places the response body directly in msg.payload"},
	{File: "network/http/02 - Handle query parameters.json", Node: "f523b8e.b5c8c48", Msg: oracleHTTPResponseMsg(oracleHTMLH1("Hello Nick!"), "84", "text/html; charset=utf-8", "200 OK", 200),
		Note: "actual-side residual: httpRequest wraps the correct body in a response object"},
	{File: "network/http/03 - Handle URL parameters.json", Node: "70c0eba4.5f0dc4", Msg: oracleHTMLH1("Hello Nick!"),
		Note: "oracle-side residual: Node-RED places the response body directly in msg.payload"},
	{File: "network/http/03 - Handle URL parameters.json", Node: "70c0eba4.5f0dc4", Msg: oracleHTTPResponseMsg(oracleHTMLH1("Hello Nick!"), "84", "text/html; charset=utf-8", "200 OK", 200),
		Note: "actual-side residual: httpRequest wraps the correct body in a response object"},
	{File: "network/http/04 - Access HTTP request headers.json", Node: "65b13d57.700074", Msg: oracleHTMLH1("User agent: Node-RED"),
		Note: "oracle-side residual: Node-RED places the response body directly in msg.payload"},
	{File: "network/http/04 - Access HTTP request headers.json", Node: "65b13d57.700074", Msg: oracleHTTPResponseMsg(oracleHTMLH1("User agent: Go-http-client/1.1"), "103", "text/html; charset=utf-8", "200 OK", 200),
		Note: "actual-side residual: httpRequest uses Go's default user agent (Node-RED sends Node-RED) and wraps the body in a response object"},
	{File: "network/http/05 - Post data to a flow.json", Node: "7c2812c.c6ee7ec", Msg: oracleHTMLH1("Hello Nick"),
		Note: "oracle-side residual: Node-RED places the response body directly in msg.payload"},
	{File: "network/http/05 - Post data to a flow.json", Node: "7c2812c.c6ee7ec", Msg: oracleHTTPResponseMsg(oracleHTMLH1("Hello Nick"), "83", "text/html; charset=utf-8", "200 OK", 200),
		Note: "actual-side residual: httpRequest wraps the correct body in a response object"},

	// --- udp: real round-trip emits an event the recorder missed ---
	{File: "network/udp/01 - Transfer data using UDP protocol.json", Node: "d98b60d3.7331e", Msg: "Hello, World!",
		Note: "actual-side residual: the real UDP datagram round-trip reaches debug; the oracle recording captured no events for this flow"},

	// --- storage: encoding, sequencing, and error-shape residuals ---
	{File: "storage/read file/02 - Read data in specified encoding.json", Node: "d28c8994.99c0a8", Msg: "8J+YgAo=",
		Note: "oracle-side residual: Node-RED honors fileIn encoding:base64"},
	{File: "storage/read file/02 - Read data in specified encoding.json", Node: "d28c8994.99c0a8", Msg: "😀\n",
		Note: "actual-side residual: fileIn emits decoded text instead of the requested base64 encoding"},
	{File: "storage/read file/03 - Read data breaking lines into messages.json", Node: "7ed8282c.92b338", Msg: "",
		Note: "oracle-side residual: Node-RED emits a trailing empty line after the final newline; ours does not"},
	{File: "storage/read file/04 - Create a message stream.json", Node: "c48d8ae0.9ff3a8", Msg: "Apple\nBanana\n",
		Note: "oracle-side residual: join + conditional switch chain emits no event — engine gap in join/switch parts semantics"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "cbfb788b.297f98", Msg: `{"__enc__":true,"type":"error","data":{"name":"Error","message":"/tmp/HG does not exist.","stack":"Error: /tmp/HG does not exist.\n    at /usr/src/node-red/node_modules/node-watch/lib/watch.js:494:9\n    at process.processTicksAndRejections (node:internal/process/task_queues:85:11)"}}`,
		Note: "oracle-side residual: watch node emits an error event for a missing watched directory (Node-RED does); ours does not"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "137d60e2.0e267f", Msg: "failed to write to file: Error: ENOENT: no such file or directory, open '/tmp/HG/hello.txt'",
		Note: "oracle-side residual: Node-RED file-write error wording for hello.txt"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "137d60e2.0e267f", Msg: "file node open failed: open /tmp/HG/hello.txt: no such file or directory",
		Note: "actual-side residual: this engine file-write error wording for hello.txt differs from Node-RED"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "655d7bab.cda6f4", Msg: "failed to write to file: Error: ENOENT: no such file or directory, open '/tmp/HG/goodbye.txt'",
		Note: "oracle-side residual: Node-RED file-write error wording for goodbye.txt"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "655d7bab.cda6f4", Msg: "file node open failed: open /tmp/HG/goodbye.txt: no such file or directory",
		Note: "actual-side residual: this engine file-write error wording for goodbye.txt differs from Node-RED"},
	{File: "storage/write file/02 - Write string to a file specified by property.json", Node: "402f3b7e.988014", Msg: "filename is empty",
		Note: "actual-side residual: inject props land in message metadata, not msg.filename — file node reports 'filename is empty'"},
	{File: "storage/write file/02 - Write string to a file specified by property.json", Node: "97b6b6b2.a54b38", Msg: "Hello, World!\n",
		Note: "oracle-side residual: because msg.filename is metadata-only, the write/read chain never produces the recorded body"},
	{File: "storage/write file/04 - Specify encoding of written data.json", Node: "2e814354.278c8c", Msg: "😀",
		Note: "oracle-side residual: Node-RED reads back the written emoji without an added newline"},
	{File: "storage/write file/04 - Specify encoding of written data.json", Node: "2e814354.278c8c", Msg: "😀\n",
		Note: "actual-side residual: written-file encoding/newline detail differs — this engine reads back a trailing newline"},

	// --- function node sandbox gap surfaced by error events ---
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:5:20(38)",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:8:20(50)",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:11:20(58)",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:14:20(64)",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},

	// --- structured parsers: object key order is unrecoverable ---
	// xml/yaml serialize from unordered Go maps (children emit sorted);
	// Node-RED keeps JavaScript object insertion order.
	{File: "parser/xml/01 - Convert JavaScript object to XML.json", Node: "1cd4ad02.9a5423", Msg: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><root><kind>Apple</kind><price>100</price><origin>Canada</origin></root>`,
		Note: "oracle-side residual: Node-RED serializes object keys in insertion order"},
	{File: "parser/xml/01 - Convert JavaScript object to XML.json", Node: "1cd4ad02.9a5423", Msg: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><root><kind>Apple</kind><origin>Canada</origin><price>100</price></root>`,
		Note: "actual-side residual: payload is an unordered Go map — children emit sorted"},
	{File: "parser/yaml/01 - Convert JavaScript object to YAML.json", Node: "1cd4ad02.9a5423", Msg: "fruits:\n  kind: Apple\n  price: 100\n  origin: Canada\n",
		Note: "oracle-side residual: js-yaml serializes object keys in insertion order"},
	{File: "parser/yaml/01 - Convert JavaScript object to YAML.json", Node: "1cd4ad02.9a5423", Msg: "fruits:\n  kind: Apple\n  origin: Canada\n  price: 100\n",
		Note: "actual-side residual: payload is an unordered Go map — yaml.v3 emits keys sorted"},
}

// oracleFlowDifferences accepts divergence for a whole flow file —
// used when a flow cannot deploy (unsupported feature) or diverges so
// broadly that per-event entries would be noise. Every entry needs a
// written reason; a tolerated flow that starts matching fails the
// suite so the map tightens as parity improves.
var oracleFlowDifferences = map[string]string{
	// --- JSONata expressions are not implemented (deploy-fails loud) ---
	"common/debug/05 - Formatting output using JSONata.json":       "JSONata expression in debug 'complete' field — no JSONata support",
	"function/change/03 - Set value using JSONata.json":            "change rule uses tot:\"jsonata\" — no JSONata support",
	"function/switch/05 - Use JSONata for switch rule.json":        "switch rule uses jsonata_exp — no JSONata support",
	"network/tcp/03 - Send reply to client of TCP connection.json": "change rule uses tot:\"jsonata\" — no JSONata support",

	// --- function node (goja) gaps ---
	"function/function/02 - Send multiple messages.json":       "Node-RED array-on-output semantics (array per output port) not implemented",
	"function/function/03 - Send messages asynchronously.json": "async done()/node.send() semantics not implemented",
	"function/function/08 - Using external modules.json":       "functionExternalModules/require() not supported in goja sandbox",
	"function/function/09 - Setup and close.json":              "function node setup/close lifecycle not implemented",
	"function/function/10 - Asynchronous setup.json":           "function node async setup lifecycle not implemented",

	// --- misc deploy gaps ---
	"network/websocket/02 - Connect to websocket out server.json": "websocket-client/websocket-listener config node types not registered",

	// --- live-network flows (real sockets/HTTP round-trips) ---
	// http/tcp/udp flows drive real listeners and round-trips via the
	// harness (shared router + ephemeral port, real sockets); residual
	// divergences are named per event in oracleDifferences.

	// --- sequence nodes ---

	// --- storage / filesystem ---
	// Fixture paths are staged into a t.TempDir by the harness (see
	// stageOracleFixtures); residual divergences are named per event in
	// oracleDifferences.
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

	accepted := map[int]bool{}         // oracleDifferences indexes actually used
	flowTolerated := map[string]bool{} // oracleFlowDifferences files actually used
	ranFlows := map[string]bool{}      // flows whose subtests were selected by -run
	var failures int
	var clean int
	var eventLevelTolerated int
	var flowLevelTolerated int
	for _, of := range orc.Flows {
		path, ok := files[of.File]
		if !ok {
			t.Errorf("oracle flow %s not present in examples dir", of.File)
			continue
		}
		of := of
		t.Run("oracle/"+sanitizeName(of.File), func(t *testing.T) {
			ranFlows[of.File] = true
			_, flowTol := oracleFlowDifferences[of.File]
			got, runErr := runOracleFlow(t, path, of)
			if runErr != nil {
				if flowTol {
					flowTolerated[of.File] = true
					flowLevelTolerated++
					return
				}
				t.Error(runErr)
				failures++
				return
			}
			beforeAccepted := len(accepted)
			missing, extra := diffOracle(t, of, got, accepted)
			if flowTol && len(missing)+len(extra) > 0 {
				flowTolerated[of.File] = true
				flowLevelTolerated++
				return
			}
			if len(accepted) > beforeAccepted {
				eventLevelTolerated++
			} else if len(missing)+len(extra) == 0 {
				clean++
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
		if !ranFlows[d.File] {
			continue // selective -run: tolerances for unexecuted flows are not missing
		}
		if !accepted[i] {
			t.Errorf("accepted difference never occurred — remove it: %s node=%s msg=%s (%s)",
				d.File, d.Node, d.Msg, d.Note)
		}
	}
	for file, note := range oracleFlowDifferences {
		if !ranFlows[file] {
			continue // selective -run: tolerances for unexecuted flows are not missing
		}
		if !flowTolerated[file] {
			t.Errorf("flow tolerance never used — remove it or investigate: %s (%s)", file, note)
		}
	}
	t.Logf("%d/%d selected oracle flows passed without accepted differences; %d used event-level differences; %d used flow-level differences; %d had unaccepted differences/errors",
		clean, len(ranFlows), eventLevelTolerated, flowLevelTolerated, failures)
}

func TestOracleDifferenceEmptyMsgDoesNotWildcardNode(t *testing.T) {
	file := "storage/read file/03 - Read data breaking lines into messages.json"
	node := "7ed8282c.92b338"
	of := oracleFlow{File: file, Debug: []oracleDebugEvent{
		{Node: node, Msg: "one"},
		{Node: node, Msg: ""},
	}}
	missing, extra := diffOracle(t, of, nil, map[int]bool{})
	if len(extra) != 0 {
		t.Fatalf("unexpected extra events: %#v", extra)
	}
	if len(missing) != 1 || missing[0].Msg != "one" {
		t.Fatalf("empty-message tolerance hid non-empty events; missing=%#v", missing)
	}
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
	// Environmental staging: flows recorded inside a Node-RED runtime
	// assume its filesystem and its single shared HTTP listener on
	// :1880. Re-create both here — fixture paths move into a private
	// temp dir, and http-in nodes mount on a shared router bound to an
	// ephemeral port (the Node-RED model: one listener, many routes).
	fixtureDir := t.TempDir()
	router, httpBase := startOracleHTTPListener(t)
	for _, n := range nodes {
		stageOracleFixtures(n, fixtureDir, httpBase)
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
	upstream := map[string]posKey{} // upstream id -> identity
	byPos := map[posKey]string{}    // identity -> upstream id
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
	// "env"-typed rule values read the process environment like real
	// Node-RED does; the recorder ran inside the Node-RED container
	// where HOME resolves to the install dir, so reproduce that here.
	t.Setenv("HOME", "/usr/src/node-red")
	engine := NewWorkflowEngine()
	engine.AllowOSEnv = true
	// Real Node-RED runs exec commands — the recorded flows depend on
	// it (function/exec/*), so the replay opts the host gate in.
	engine.AllowExec = true
	engine.HTTPRouter = router
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
		// Traversals run in the background; the recorder clicked
		// injects seconds apart, so let each one settle before firing
		// the next — back-to-back fires would race on shared flow
		// context.
		time.Sleep(100 * time.Millisecond)
	}
	// Watch nodes report filesystem changes asynchronously — give them
	// a wider settle window than message-driven flows need. Rate-limited
	// delays and join timeouts also flush on wall-clock timers (the
	// oracle's real-time recording captures them), so widen the settle
	// window when a flow uses them.
	//
	// Timer-driven nodes (trigger durations, delay holds) are mirrored
	// at the recorder's own capture window: the oracle contains
	// trigger/01's 2s release but not delay/01's 3s one, so the
	// recorder waited roughly 2-3s after the last inject — settle for
	// 2.5s matches what real Node-RED was observed doing inside that
	// window. Longer configured holds (delay/03's minute, delay/02's
	// 10s msg.delay) stay unreleased exactly as they did during the
	// recording.
	settle := 500 * time.Millisecond
	widen := func(d time.Duration) {
		if d > settle {
			settle = d
		}
	}
	for _, n := range nodes {
		switch n["type"] {
		case "watch":
			widen(2500 * time.Millisecond)
		case "trigger", "delay":
			widen(2500 * time.Millisecond)
		case "join":
			if to, ok := n["timeout"]; ok {
				if f, err := strconv.ParseFloat(fmt.Sprint(to), 64); err == nil && f > 0 {
					widen(8 * time.Second)
				}
			}
		}
	}
	time.Sleep(settle)

	_, _, snap, _ := engine.GetWorkflowSnapshot(w.GetID(), 0)
	var got []oracleDebugEvent
	fixturePrefix := filepath.ToSlash(fixtureDir)
	for _, ev := range snap {
		// The oracle records both debug output and node-level errors
		// (the recorder captures what Node-RED surfaces per node). Fixture
		// paths are harness artifacts: map the private temp dir back onto
		// Node-RED's /tmp before comparing event text.
		if (ev.Level == "debug" || ev.Level == "error") && ev.NodeID != "" {
			msg := strings.ReplaceAll(ev.Message, fixturePrefix, "/tmp")
			got = append(got, oracleDebugEvent{Node: importedToUpstream[ev.NodeID], Name: ev.NodeName, Msg: msg})
		}
	}
	return got, nil
}

// oracleSharedRouter adapts a shared http.ServeMux to
// nodebase.HTTPRouter so every http-in node in a flow mounts on one
// listener — the Node-RED model. Conflicts are returned as errors
// (deploy failures), never panics; Unregister rebuilds the mux without
// the removed pattern (ServeMux itself cannot unregister) and
// tolerates patterns that were never mounted.
type oracleSharedRouter struct {
	mu       sync.Mutex
	mux      *http.ServeMux // swapped atomically under mu on rebuild
	handlers map[string]http.Handler
}

func (r *oracleSharedRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	mux := r.mux
	r.mu.Unlock()
	mux.ServeHTTP(w, req)
}

func (r *oracleSharedRouter) rebuild() {
	mux := http.NewServeMux()
	for pattern, h := range r.handlers {
		mux.Handle(pattern, h)
	}
	r.mux = mux
}

func (r *oracleSharedRouter) Handle(pattern string, h http.Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[pattern]; ok {
		return fmt.Errorf("route %q already mounted", pattern)
	}
	// ServeMux.Handle panics on a conflicting pattern — probe on a
	// throwaway mux and translate the panic into the contract error.
	var panErr error
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panErr = fmt.Errorf("%v", rec)
			}
		}()
		probe := http.NewServeMux()
		for p, ph := range r.handlers {
			probe.Handle(p, ph)
		}
		probe.Handle(pattern, h)
	}()
	if panErr != nil {
		return fmt.Errorf("route %q: %w", pattern, panErr)
	}
	r.handlers[pattern] = h
	r.rebuild()
	return nil
}

func (r *oracleSharedRouter) Unregister(pattern string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[pattern]; !ok {
		return
	}
	delete(r.handlers, pattern)
	r.rebuild()
}

// startOracleHTTPListener binds a loopback listener on an ephemeral
// port serving a fresh shared router — the stand-in for the Node-RED
// runtime's own HTTP listener. It returns the router (installed on the
// engine as host.SharedHTTPRouter) and the base URL request nodes
// should be rewritten to. Subtests run sequentially and each gets its
// own port, so same-port bind races cannot occur.
func startOracleHTTPListener(t *testing.T) (nodebase.HTTPRouter, string) {
	t.Helper()
	router := &oracleSharedRouter{mux: http.NewServeMux(), handlers: map[string]http.Handler{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("oracle http listener: %v", err)
	}
	srv := &http.Server{Handler: router}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return router, fmt.Sprintf("http://localhost:%d", ln.Addr().(*net.TCPAddr).Port)
}

// stageOracleFixtures rewrites one raw Node-RED node document so the
// flow runs hermetically:
//
//   - filesystem fixtures: Node-RED examples read/write under /tmp —
//     paths in "filename"/"files" config and inject props are moved
//     under fixtureDir, preserving the relative structure (a missing
//     dir in the recording stays missing here).
//   - http-in nodes are switched to listener "main" so they mount on
//     the shared router instead of each binding a private port.
//   - request URLs pointing at the recorded :1880 listener (node "url"
//     config and inject props) are rewritten to the live listener.
func stageOracleFixtures(n map[string]interface{}, fixtureDir, httpBase string) {
	movePath := func(s string) string {
		base := filepath.ToSlash(fixtureDir)
		if s == "/tmp" {
			return base
		}
		if strings.HasPrefix(s, "/tmp/") {
			return filepath.ToSlash(filepath.Join(base, s[len("/tmp/"):]))
		}
		return s
	}
	moveURL := func(s string) string {
		for _, host := range []string{"localhost:1880", "127.0.0.1:1880"} {
			if strings.Contains(s, "://"+host) {
				return strings.Replace(s, "://"+host, "://"+strings.TrimPrefix(httpBase, "http://"), 1)
			}
		}
		return s
	}

	for _, key := range []string{"filename", "files"} {
		if s, ok := n[key].(string); ok {
			n[key] = movePath(s)
		}
	}
	switch n["type"] {
	case "http in":
		// Mount on the shared router; Port is ignored in "main" mode.
		n["listener"] = "main"
		// Node-RED stores the method lowercase ("get"); ServeMux
		// method patterns match exact case, so normalize.
		if s, ok := n["method"].(string); ok {
			n["method"] = strings.ToUpper(s)
		}
	case "http request":
		if s, ok := n["url"].(string); ok {
			n["url"] = moveURL(s)
		}
	case "inject":
		for _, p := range arrayOfMaps(n["props"]) {
			// Node-RED semantics: a "payload" prop with no v/vt means
			// "use the legacy payload/payloadType fields" — backfill
			// so the inject does not fall back to a timestamp.
			if p["p"] == "payload" {
				_, hasV := p["v"]
				_, hasVT := p["vt"]
				if !hasV && !hasVT {
					if s, ok := n["payload"].(string); ok && s != "" {
						p["v"] = s
						vt, _ := n["payloadType"].(string)
						if vt == "" {
							vt = "str"
						}
						p["vt"] = vt
					}
				}
			}
			if s, ok := p["v"].(string); ok {
				p["v"] = moveURL(movePath(s))
			}
		}
	}
}

// arrayOfMaps returns the elements of v that are JSON objects.
func arrayOfMaps(v interface{}) []map[string]interface{} {
	items, _ := v.([]interface{})
	var out []map[string]interface{}
	for _, it := range items {
		if m, ok := it.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
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
			if normOracleMsg(d.Msg) != normOracleMsg(ev.Msg) {
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
	scrubOracleVolatile(v)
	raw, err := json.Marshal(v)
	if err != nil {
		return s
	}
	return string(raw)
}

// scrubOracleVolatile removes harness/runtime timestamps that are not
// behavioral differences: http-request durations and Date response
// headers change on every run, even when the flow is correct.
func scrubOracleVolatile(v interface{}) {
	switch x := v.(type) {
	case map[string]interface{}:
		// Only the http-request response wrapper has volatile runtime
		// metadata. Do not scrub ordinary payloads that happen to contain
		// a meaningful "duration" field.
		_, hasBody := x["body"]
		_, hasBodyTruncated := x["bodyTruncated"]
		_, hasStatus := x["status"]
		_, hasStatusCode := x["statusCode"]
		headers, hasHeaders := x["headers"].(map[string]interface{})
		if hasBody && hasBodyTruncated && hasStatus && hasStatusCode && hasHeaders {
			delete(x, "duration")
			delete(headers, "Date")
		}
		for _, child := range x {
			scrubOracleVolatile(child)
		}
	case []interface{}:
		for _, child := range x {
			scrubOracleVolatile(child)
		}
	}
}

func trunc(s string, n int) string {
	if len(s) > n {
		return fmt.Sprintf("%s…(%d bytes)", s[:n], len(s))
	}
	return s
}
