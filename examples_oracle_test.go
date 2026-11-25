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
	// --- http: response payload shape & request-context gaps ---
	// httpRequest emits a response object {body, statusCode, headers,…}
	// while Node-RED puts the response body directly in msg.payload
	// (ret:"txt"), so the recorded html body and the emitted JSON map
	// never match on these debug nodes.
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "18b48ed8.1072e1", Msg: "",
		Note: "httpRequest wraps the body in a response object; Node-RED places it in msg.payload"},
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "188bc18.5487a3f", Msg: "",
		Note: "same response-object wrapper; also msg.url from inject props lands in metadata, not payload"},
	{File: "network/http/01 - Create HTTP endpoint.json", Node: "", Msg: "invalid input data format",
		Note: "second httpRequest rejects the inject message — msg.url arrives in metadata, not the payload map"},
	{File: "network/http/02 - Handle query parameters.json", Node: "f523b8e.b5c8c48", Msg: "",
		Note: "query parameters are not exposed to the template as req.query.*; response is the wrapper object"},
	{File: "network/http/03 - Handle URL parameters.json", Node: "70c0eba4.5f0dc4", Msg: "",
		Note: "route parameters are not exposed to the template as req.params.*; response is the wrapper object"},
	{File: "network/http/04 - Access HTTP request headers.json", Node: "65b13d57.700074", Msg: "",
		Note: "request headers are not exposed to the template as req.headers.*; response is the wrapper object"},
	{File: "network/http/05 - Post data to a flow.json", Node: "7c2812c.c6ee7ec", Msg: "",
		Note: "httpTrigger JSON-decodes request bodies — a plain-text POST is rejected with 400 (Node-RED accepts raw bodies)"},

	// --- udp: real round-trip emits an event the recorder missed ---
	{File: "network/udp/01 - Transfer data using UDP protocol.json", Node: "d98b60d3.7331e", Msg: "",
		Note: "the real UDP datagram round-trip reaches debug; the oracle recording captured no events for this flow"},

	// --- storage: fixture-path & encoding residuals ---
	{File: "storage/read file/02 - Read data in specified encoding.json", Node: "d28c8994.99c0a8", Msg: "",
		Note: "fileIn 'encoding':'base64' output encoding is not applied — payload is the decoded text"},
	{File: "storage/read file/03 - Read data breaking lines into messages.json", Node: "7ed8282c.92b338", Msg: "",
		Note: "Node-RED emits a trailing empty line after the final newline; ours does not (wildcard also covers the matching lines)"},
	{File: "storage/read file/04 - Create a message stream.json", Node: "c48d8ae0.9ff3a8", Msg: "",
		Note: "join + conditional switch chain emits no event — engine gap in join/switch parts semantics"},
	{File: "storage/watch/01 - Watch change of a file.json", Node: "a91562b9.ca805", Msg: "",
		Note: "watch reports the absolute fixture path; the harness stages it under t.TempDir"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "cbfb788b.297f98", Msg: "",
		Note: "watch node emits no error event for a missing watched directory (Node-RED does)"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "137d60e2.0e267f", Msg: "",
		Note: "file-write error text embeds the staged temp path and differs in wording from Node-RED"},
	{File: "storage/watch/02 - Watch change in a directory.json", Node: "655d7bab.cda6f4", Msg: "",
		Note: "file-write error text embeds the staged temp path and differs in wording from Node-RED"},
	{File: "storage/write file/02 - Write string to a file specified by property.json", Node: "402f3b7e.988014", Msg: "",
		Note: "inject props land in message metadata, not msg.filename — file node reports 'filename is empty'"},
	{File: "storage/write file/02 - Write string to a file specified by property.json", Node: "97b6b6b2.a54b38", Msg: "",
		Note: "consequence of the above: the read side never produces the recorded body"},
	{File: "storage/write file/04 - Specify encoding of written data.json", Node: "2e814354.278c8c", Msg: "",
		Note: "written-file encoding/newline detail differs (msg text identical modulo invisible bytes)"},

	// --- function node sandbox gap surfaced by error events ---
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:5:20",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:8:20",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:11:20",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
	{File: "function/function/07 - Showing status information.json", Node: "a4a6c205.8afd4", Msg: "function script error: TypeError: Object has no member 'status' at function:14:20",
		Note: "goja sandbox lacks node.status — errors Node-RED never raised"},
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

	// --- env vars / flow & global context (deploy-fails loud) ---
	"function/change/04 - Set value from env var.json":               "change rule reads process env (tot:\"env\") — not supported",
	"function/change/05 - Set flow context.json":                     "change rule writes flow context — change/switch rules only see msg",
	"function/switch/09 - Route message based on context value.json": "change rule writes flow context — not supported",

	// --- function node (goja) gaps ---
	"function/function/02 - Send multiple messages.json":       "Node-RED array-on-output semantics (array per output port) not implemented",
	"function/function/03 - Send messages asynchronously.json": "async done()/node.send() semantics not implemented",
	"function/function/08 - Using external modules.json":       "functionExternalModules/require() not supported in goja sandbox",
	"function/function/09 - Setup and close.json":              "function node setup/close lifecycle not implemented",
	"function/function/10 - Asynchronous setup.json":           "function node async setup lifecycle not implemented",

	// --- switch / range evaluation semantics ---
	"function/switch/02 - Check all rules.json":                   "\"check all\" mode output semantics diverge",
	"function/switch/06 - Use JSONata for switch value.json":      "odd/even routing via boolean rule diverges",
	"function/switch/07 - Recreate message sequence.json":         "message-sequence (parts) recreation not implemented",
	"function/switch/08 - Route message based on properties.json": "multi-property routing diverges",

	// --- delay timing semantics ---
	"function/delay/01 - Delay message.json":                     "delay timing window exceeds oracle capture window",
	"function/delay/02 - Delay message by message property.json": "msg.delay timing semantics diverge",
	"function/delay/03 - Reset or flush pending message.json":    "delay reset/flush semantics diverge",

	// --- template formatting diffs ---
	"function/template/01 - Use mustache syntax.json":  "mustache rendering whitespace/output differs",
	"function/template/02 - Parse result as JSON.json": "JSON parse of rendered template diverges",
	"function/template/03 - Parse result as YAML.json": "YAML parse of rendered template diverges",

	// --- misc deploy gaps ---
	"function/trigger/01 - Outputs two value with interval.json":                               "NR serializes bytopic as \"all\" string — enum-valued bool not coerced",
	"network/websocket/02 - Connect to websocket out server.json":                              "websocket-client/websocket-listener config node types not registered",
	"parser/html/03 - Extract array of HTML element by CSS selector specified in message.json": "html selector from msg not supported (config requires tag)",

	// --- live-network flows (real sockets/HTTP round-trips) ---
	// http/tcp/udp flows drive real listeners and round-trips via the
	// harness (shared router + ephemeral port, real sockets); residual
	// divergences are named per event in oracleDifferences.

	// --- exec node ---
	"function/exec/01 - Get standard output from external command.json": "exec output/exit semantics diverge from Node-RED spawn",
	"function/exec/02 - Get error output from external command.json":    "exec stderr semantics diverge",
	"function/exec/03 - Run external command in spawn mode.json":        "exec spawn mode semantics diverge",

	// --- link nodes ---
	"common/link/02 - Link across tabs.json": "link across tabs routing not implemented",
	"common/link/03 - Link call.json":        "linkCall return-value semantics diverge",

	// --- csv parser ---
	"parser/csv/01 - Parse CSV with default column name as message sequence.json":             "CSV message-sequence emission semantics diverge",
	"parser/csv/02 - Parse CSV with default column name as array.json":                        "CSV array output format diverges",
	"parser/csv/03 - Parse CSV with specified column name as message sequence.json":           "CSV named-column sequence diverges",
	"parser/csv/04 - Parse CSV with column name in first row as message sequence.json":        "CSV header-row column names diverge",
	"parser/csv/05 - Convert JavaScript object to CSV.json":                                   "object-to-CSV column selection diverges",
	"parser/csv/06 - Convert JavaScript object to CSV.json":                                   "object-to-CSV column selection diverges",
	"parser/csv/07 - Convert array of JavaScript objects to CSV with column name header.json": "CSV header output diverges",
	"parser/csv/08 - Specify column names in input message.json":                              "msg-provided column names diverge",
	"parser/csv/09 - Send column name when reset property set.json":                           "CSV reset-property column emission diverges",
	"parser/csv/10 - Join parsed CSV message sequence using join node.json":                   "CSV+join sequence semantics diverge",

	// --- html parser ---
	"parser/html/01 - Extract array of HTML element by CSS selector.json":           "html extraction output shape diverges (elements vs text)",
	"parser/html/02 - Extract sequence of HTML element by CSS selector.json":        "html sequence extraction diverges",
	"parser/html/04 - Join extracted sequence of HTML element using join node.json": "html+join sequence diverges",

	// --- structured parsers ---
	"parser/json/03 - Validate input JSON string.json":               "json schema validation not implemented",
	"parser/xml/01 - Convert JavaScript object to XML.json":          "object-to-XML serialization diverges",
	"parser/xml/02 - Convert XML to JavaScript object.json":          "XML-to-object conversion diverges",
	"parser/xml/03 - Control conversion using options property.json": "msg-controlled XML options diverge",
	"parser/yaml/01 - Convert JavaScript object to YAML.json":        "object-to-YAML serialization diverges",

	// --- sequence nodes ---
	"sequence/batch/01 - Number-based group mode.json": "batch grouping sequence semantics diverge",
	"sequence/batch/03 - Concatenate mode.json":        "batch concatenate semantics diverge",
	"sequence/join/01 - Automatic join mode.json":      "join auto-mode output diverges",
	"sequence/join/02 - Manual join mode.json":         "join manual-mode (count/timeout) semantics diverge",
	"sequence/join/03 - Reduce sequence mode.json":     "join reduce-expression semantics diverge",
	"sequence/sort/01 - Sort array payload.json":       "sort ordering/key semantics diverge",
	"sequence/sort/02 - Sort message sequence.json":    "sequence sort (parts) semantics diverge",

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
	engine := NewWorkflowEngine()
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
	}
	// Watch nodes report filesystem changes asynchronously — give them
	// a wider settle window than message-driven flows need.
	settle := 500 * time.Millisecond
	for _, n := range nodes {
		if n["type"] == "watch" {
			settle = 2500 * time.Millisecond
		}
	}
	time.Sleep(settle)

	_, _, snap, _ := engine.GetWorkflowSnapshot(w.GetID(), 0)
	var got []oracleDebugEvent
	for _, ev := range snap {
		// The oracle records both debug output and node-level errors
		// (the recorder captures what Node-RED surfaces per node).
		if (ev.Level == "debug" || ev.Level == "error") && ev.NodeID != "" {
			got = append(got, oracleDebugEvent{Node: importedToUpstream[ev.NodeID], Name: ev.NodeName, Msg: ev.Message})
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
		if s == "/tmp" {
			return fixtureDir
		}
		if strings.HasPrefix(s, "/tmp/") {
			return filepath.Join(fixtureDir, s[len("/tmp/"):])
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
