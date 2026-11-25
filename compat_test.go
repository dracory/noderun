package noderunner

// Test-only compatibility shim for the mechanically ported
// pkg/flowrunner test suite (cutover P6). The old suite was written
// against flowrunner's concrete value types (Workflow/Node/Edge/
// Message structs) and an engine that consumed them directly; the new
// stack speaks nodestore interfaces. To keep ported test bodies
// verbatim, this file re-exposes the old shapes and adapts them at
// the engine seam. *Message implements nodestore.MessageInterface so
// ported literals can be passed wherever the engine wants a message.

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	_ "github.com/dracory/nodebase/builtin"
	nodestore "github.com/dracory/nodestore"

	batchnode "github.com/dracory/nodebase/batch"
	changenode "github.com/dracory/nodebase/change"
	commentnode "github.com/dracory/nodebase/comment"
	conditionnode "github.com/dracory/nodebase/condition"
	csvnode "github.com/dracory/nodebase/csv"
	execnode "github.com/dracory/nodebase/exec"
	fsoutnode "github.com/dracory/nodebase/fsout"

	htmlnode "github.com/dracory/nodebase/html"
	httprequestnode "github.com/dracory/nodebase/httprequest"
	httptriggernode "github.com/dracory/nodebase/httptrigger"
	injectnode "github.com/dracory/nodebase/inject"
	joinnode "github.com/dracory/nodebase/join"
	jsonnode "github.com/dracory/nodebase/json"
	linkcallnode "github.com/dracory/nodebase/linkcall"
	mailoutnode "github.com/dracory/nodebase/mailout"
	mqttinnode "github.com/dracory/nodebase/mqttin"
	rangenode "github.com/dracory/nodebase/range"
	rbencode "github.com/dracory/nodebase/rbe"
	sortnode "github.com/dracory/nodebase/sort"
	splitnode "github.com/dracory/nodebase/split"
	submitnode "github.com/dracory/nodebase/submitin"
	switchnode "github.com/dracory/nodebase/switch"
	tcprequestnode "github.com/dracory/nodebase/tcprequest"
	templatenode "github.com/dracory/nodebase/template"
	tlsconfignode "github.com/dracory/nodebase/tlsconfig"
	triggernode "github.com/dracory/nodebase/trigger"
	xmlnode "github.com/dracory/nodebase/xml"
)

// == OLD VALUE TYPES =========================================================
// Mirrors of flowrunner's Workflow/Node/Edge/Message structs — same
// field names so ported literals compile unchanged.

// Node mirrors flowrunner.Node (a stored node definition) and
// implements nodestore.NodeInterface lazily like Workflow.
type Node struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Data    map[string]interface{} `json:"data"`
	GroupID string                 `json:"group_id,omitempty"`

	inner nodestore.NodeInterface
}

func (n *Node) store() nodestore.NodeInterface {
	if n.inner == nil {
		n.inner = nodestore.NewNode(n.ID, n.Type).SetData(n.Data).SetGroupID(n.GroupID)
	}
	return n.inner
}

func (n *Node) GetID() string { return n.store().GetID() }
func (n *Node) SetID(id string) nodestore.NodeInterface {
	n.ID = id
	return n.store().SetID(id)
}
func (n *Node) GetWorkflowID() string { return n.store().GetWorkflowID() }
func (n *Node) SetWorkflowID(id string) nodestore.NodeInterface {
	return n.store().SetWorkflowID(id)
}
func (n *Node) GetGroupID() string { return n.store().GetGroupID() }
func (n *Node) SetGroupID(id string) nodestore.NodeInterface {
	n.GroupID = id
	return n.store().SetGroupID(id)
}
func (n *Node) GetType() string { return n.store().GetType() }
func (n *Node) SetType(t string) nodestore.NodeInterface {
	n.Type = t
	return n.store().SetType(t)
}
func (n *Node) GetPositionX() float64 { return n.store().GetPositionX() }
func (n *Node) SetPositionX(x float64) nodestore.NodeInterface {
	return n.store().SetPositionX(x)
}
func (n *Node) GetPositionY() float64 { return n.store().GetPositionY() }
func (n *Node) SetPositionY(y float64) nodestore.NodeInterface {
	return n.store().SetPositionY(y)
}
func (n *Node) SetPosition(x, y float64) nodestore.NodeInterface {
	return n.store().SetPosition(x, y)
}
func (n *Node) GetData() map[string]interface{} { return n.store().GetData() }
func (n *Node) SetData(d map[string]interface{}) nodestore.NodeInterface {
	n.Data = d
	return n.store().SetData(d)
}
func (n *Node) GetCreatedAt() time.Time { return n.store().GetCreatedAt() }
func (n *Node) SetCreatedAt(t time.Time) nodestore.NodeInterface {
	return n.store().SetCreatedAt(t)
}
func (n *Node) GetUpdatedAt() time.Time { return n.store().GetUpdatedAt() }
func (n *Node) SetUpdatedAt(t time.Time) nodestore.NodeInterface {
	return n.store().SetUpdatedAt(t)
}

var _ nodestore.NodeInterface = (*Node)(nil)

// Edge mirrors flowrunner.Edge.
type Edge struct {
	ID        string `json:"id"`
	Source    string `json:"source"`
	SourceKey string `json:"source_key,omitempty"`
	Target    string `json:"target"`
	TargetKey string `json:"target_key,omitempty"`
}

// Workflow mirrors flowrunner.Workflow.
type Workflow struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Nodes       []Node                 `json:"nodes"`
	Edges       []Edge                 `json:"edges"`
	View        map[string]interface{} `json:"view,omitempty"`
	Deployed    bool                   `json:"deployed,omitempty"`
	CreatedAt   time.Time              `json:"createdAt"`
	UpdatedAt   time.Time              `json:"updatedAt"`

	inner nodestore.WorkflowInterface
}

// Message mirrors flowrunner.Message and implements
// nodestore.MessageInterface so it can cross the engine seam.
type Message struct {
	ID        string                 `json:"id,omitempty"`
	Payload   interface{}            `json:"payload,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Error     *string                `json:"error,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// NewMessage mirrors flowrunner.NewMessage (ported tests construct
// &Message{...} literals, but a few call the constructor).
func NewMessage() *Message {
	return &Message{
		ID:        nodebase.GenerateID(),
		Payload:   map[string]interface{}{},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now(),
	}
}

// -- nodestore.MessageInterface implementation ---------------------------

func (m *Message) GetID() string { return m.ID }
func (m *Message) SetID(id string) nodestore.MessageInterface {
	m.ID = id
	return m
}

func (m *Message) GetPayload() interface{} { return m.Payload }
func (m *Message) SetPayload(p interface{}) nodestore.MessageInterface {
	m.Payload = p
	return m
}

func (m *Message) GetMetadata() map[string]interface{} { return m.Metadata }
func (m *Message) SetMetadata(md map[string]interface{}) nodestore.MessageInterface {
	m.Metadata = md
	return m
}

func (m *Message) GetError() *string { return m.Error }
func (m *Message) SetError(err string) nodestore.MessageInterface {
	m.Error = &err
	return m
}
func (m *Message) ClearError() nodestore.MessageInterface {
	m.Error = nil
	return m
}

func (m *Message) GetTimestamp() time.Time { return m.Timestamp }
func (m *Message) SetTimestamp(ts time.Time) nodestore.MessageInterface {
	m.Timestamp = ts
	return m
}

func (m *Message) GetCreatedAt() time.Time { return m.Timestamp }
func (m *Message) SetCreatedAt(ts time.Time) nodestore.MessageInterface {
	m.Timestamp = ts
	return m
}
func (m *Message) GetUpdatedAt() time.Time { return m.Timestamp }
func (m *Message) SetUpdatedAt(ts time.Time) nodestore.MessageInterface {
	m.Timestamp = ts
	return m
}

var _ nodestore.MessageInterface = (*Message)(nil)

// == CONVERSIONS =============================================================

// toStore converts the shim Workflow literal to a nodestore workflow.
func (w *Workflow) toStore() nodestore.WorkflowInterface {
	wf := nodestore.NewWorkflow(w.Name).SetID(w.ID)
	ns := make([]nodestore.NodeInterface, 0, len(w.Nodes))
	for _, n := range w.Nodes {
		ns = append(ns, nodestore.NewNode(n.ID, n.Type).
			SetWorkflowID(w.ID).SetData(n.Data))
	}
	es := make([]nodestore.EdgeInterface, 0, len(w.Edges))
	for _, e := range w.Edges {
		es = append(es, nodestore.NewEdge(e.ID, e.Source, e.Target).
			SetWorkflowID(w.ID).SetSourceKey(e.SourceKey).SetTargetKey(e.TargetKey))
	}
	wf.SetNodes(ns).SetEdges(es)
	if w.View != nil {
		wf.SetView(w.View)
	}
	wf.SetDescription(w.Description).SetDeployed(w.Deployed)
	return wf
}

// store returns the backing nodestore workflow, rebuilt on every call
// so field mutations (w.Nodes[i].Data = ..., w.View[...] = ...) stay
// visible to the engine — e.g. partial-deploy diffing. Rebuilding is
// cheap for test-sized flows.
func (w *Workflow) store() nodestore.WorkflowInterface {
	return w.toStore()
}

// -- nodestore.WorkflowInterface implementation ---------------------------

func (w *Workflow) GetID() string { return w.store().GetID() }
func (w *Workflow) SetID(id string) nodestore.WorkflowInterface {
	w.ID = id
	return w.store().SetID(id)
}
func (w *Workflow) GetName() string { return w.store().GetName() }
func (w *Workflow) SetName(name string) nodestore.WorkflowInterface {
	w.Name = name
	return w.store().SetName(name)
}
func (w *Workflow) GetDescription() string { return w.store().GetDescription() }
func (w *Workflow) SetDescription(d string) nodestore.WorkflowInterface {
	w.Description = d
	return w.store().SetDescription(d)
}
func (w *Workflow) GetNodes() []nodestore.NodeInterface { return w.store().GetNodes() }
func (w *Workflow) SetNodes(ns []nodestore.NodeInterface) nodestore.WorkflowInterface {
	return w.store().SetNodes(ns)
}
func (w *Workflow) AddNode(n nodestore.NodeInterface) nodestore.WorkflowInterface {
	return w.store().AddNode(n)
}
func (w *Workflow) FindNodeByID(id string) nodestore.NodeInterface {
	return w.store().FindNodeByID(id)
}
func (w *Workflow) GetEdges() []nodestore.EdgeInterface { return w.store().GetEdges() }
func (w *Workflow) SetEdges(es []nodestore.EdgeInterface) nodestore.WorkflowInterface {
	return w.store().SetEdges(es)
}
func (w *Workflow) AddEdge(e nodestore.EdgeInterface) nodestore.WorkflowInterface {
	return w.store().AddEdge(e)
}
func (w *Workflow) GetView() map[string]interface{} { return w.store().GetView() }
func (w *Workflow) SetView(v map[string]interface{}) nodestore.WorkflowInterface {
	w.View = v
	return w.store().SetView(v)
}
func (w *Workflow) GetDeployed() bool { return w.store().GetDeployed() }
func (w *Workflow) SetDeployed(d bool) nodestore.WorkflowInterface {
	w.Deployed = d
	return w.store().SetDeployed(d)
}
func (w *Workflow) GetCreatedAt() time.Time { return w.store().GetCreatedAt() }
func (w *Workflow) SetCreatedAt(t time.Time) nodestore.WorkflowInterface {
	w.CreatedAt = t
	return w.store().SetCreatedAt(t)
}
func (w *Workflow) GetUpdatedAt() time.Time { return w.store().GetUpdatedAt() }
func (w *Workflow) SetUpdatedAt(t time.Time) nodestore.WorkflowInterface {
	w.UpdatedAt = t
	return w.store().SetUpdatedAt(t)
}

var _ nodestore.WorkflowInterface = (*Workflow)(nil)

// asStoreWorkflow accepts either a *Workflow shim or a
// nodestore.WorkflowInterface (hand-built or engine-returned).
func asStoreWorkflow(w interface{}) nodestore.WorkflowInterface {
	switch v := w.(type) {
	case nil:
		return nil
	case nodestore.WorkflowInterface:
		return v
	case *Workflow:
		return v.toStore()
	case Workflow:
		return (&v).toStore()
	default:
		panic("compat_test: unsupported workflow type")
	}
}

// asStoreMessage accepts *Message, nodestore.MessageInterface or nil.
func asStoreMessage(m interface{}) nodestore.MessageInterface {
	switch v := m.(type) {
	case nil:
		return nil
	case nodestore.MessageInterface:
		return v
	case Message:
		return &v
	default:
		panic("compat_test: unsupported message type")
	}
}

// shimMsg re-boxes a nodestore message as the shim *Message (a no-op
// when it already is one).
func shimMsg(m nodestore.MessageInterface) *Message {
	if m == nil {
		return nil
	}
	if sm, ok := m.(*Message); ok {
		return sm
	}
	return &Message{
		ID:        m.GetID(),
		Payload:   m.GetPayload(),
		Metadata:  m.GetMetadata(),
		Error:     m.GetError(),
		Timestamp: m.GetTimestamp(),
	}
}

// == ENGINE SHIM =============================================================

// Engine wraps *WorkflowEngine, re-exposing the
// flowrunner signatures the ported tests were written against. All
// other methods and fields promote through the embedded engine.
type Engine struct {
	*WorkflowEngine
}

// NewEngine mirrors flowrunner.NewWorkflowEngine.
func NewEngine() *Engine {
	return &Engine{WorkflowEngine: NewWorkflowEngine()}
}

// ExecuteWorkflow deploys and starts the workflow (shim or interface).
func (e *Engine) ExecuteWorkflow(ctx context.Context, workflow interface{}, input interface{}) (string, error) {
	return e.WorkflowEngine.ExecuteWorkflow(ctx, asStoreWorkflow(workflow), asStoreMessage(input))
}

// DeployWorkflow mirrors flowrunner.WorkflowEngine.DeployWorkflow.
func (e *Engine) DeployWorkflow(ctx context.Context, workflow interface{}, input interface{}) (string, error) {
	return e.WorkflowEngine.DeployWorkflow(ctx, asStoreWorkflow(workflow), asStoreMessage(input))
}

// DeployWorkflowPartial mirrors flowrunner.WorkflowEngine.DeployWorkflowPartial.
func (e *Engine) DeployWorkflowPartial(ctx context.Context, workflow interface{}, input interface{}) (string, error) {
	return e.WorkflowEngine.DeployWorkflowPartial(ctx, asStoreWorkflow(workflow), asStoreMessage(input))
}

// == NODE SHIMS ==============================================================
// Aliases + old-signature constructors so ported test bodies that built
// nodes directly (NewXNode(id, cfg, engine, workflowID)) compile
// verbatim against the nodes/<type> packages.

type (
	BatchConfig       = batchnode.BatchConfig
	CSVConfig         = csvnode.CSVConfig
	ChangeConfig      = changenode.ChangeConfig
	CommentConfig     = commentnode.CommentConfig
	ConditionConfig   = conditionnode.ConditionConfig
	ExecConfig        = execnode.ExecConfig
	FsOutConfig       = fsoutnode.FsOutConfig
	HTMLConfig        = htmlnode.HTMLConfig
	HTTPRequestConfig = httprequestnode.HTTPRequestConfig
	HTTPTriggerConfig = httptriggernode.HTTPTriggerConfig
	InjectConfig      = injectnode.InjectConfig
	JSONConfig        = jsonnode.JSONConfig
	JoinConfig        = joinnode.JoinConfig
	LinkCallConfig    = linkcallnode.LinkCallConfig
	MailOutConfig     = mailoutnode.MailOutConfig
	MQTTInConfig      = mqttinnode.MQTTInConfig
	RangeConfig       = rangenode.RangeConfig
	RBEConfig         = rbencode.RBEConfig
	RBENode           = rbencode.RBENode
	SortConfig        = sortnode.SortConfig
	SplitConfig       = splitnode.SplitConfig
	SubmitConfig      = submitnode.SubmitConfig
	SwitchConfig      = switchnode.SwitchConfig
	SwitchRule        = nodebase.SwitchRule
	TCPRequestConfig  = tcprequestnode.TCPRequestConfig
	TemplateConfig    = templatenode.TemplateConfig
	TLSConfig         = nodebase.TLSConfig
	TriggerConfig     = triggernode.TriggerConfig
	XMLConfig         = xmlnode.XMLConfig
)

// stNode builds the stored-node definition the new constructors want.
func stNode(id, nodeType, workflowID string) nodestore.NodeInterface {
	n := nodestore.NewNode(id, nodeType)
	if workflowID != "" {
		n.SetWorkflowID(workflowID)
	}
	return n
}

func NewBatchNode(id string, config *BatchConfig, engine nodebase.NodeHost, workflowID string) *batchnode.BatchNode {
	return batchnode.NewBatchNode(stNode(id, "batch", workflowID), config, engine)
}

func NewCSVNode(id string, config *CSVConfig) *csvnode.CSVNode {
	return csvnode.NewCSVNode(stNode(id, "csv", ""), config)
}

func NewChangeNode(id string, config *ChangeConfig) *changenode.ChangeNode {
	return changenode.NewChangeNode(stNode(id, "change", ""), config)
}

func NewCommentNode(id string, config *CommentConfig) *commentnode.CommentNode {
	return commentnode.NewCommentNode(stNode(id, "comment", ""), config)
}

func NewConditionNode(id string, config *ConditionConfig) *conditionnode.ConditionNode {
	return conditionnode.NewConditionNode(stNode(id, "condition", ""), config)
}

func NewExecNode(id string, config *ExecConfig, engine nodebase.NodeHost) *execnode.ExecNode {
	return execnode.NewExecNode(stNode(id, "exec", ""), config, engine)
}

func NewFsOutNode(id string, config *FsOutConfig, engine nodebase.NodeHost) *fsoutnode.FsOutNode {
	return fsoutnode.NewFsOutNode(stNode(id, "fsOut", ""), config, engine)
}

func NewHTMLNode(id string, config *HTMLConfig) *htmlnode.HTMLNode {
	return htmlnode.NewHTMLNode(stNode(id, "html", ""), config)
}

func NewHTTPRequest(id string, config *HTTPRequestConfig) *httprequestnode.HTTPRequest {
	return httprequestnode.NewHTTPRequest(stNode(id, "httpRequest", ""), config)
}

func NewHTTPTrigger(id string, config *HTTPTriggerConfig, engine nodebase.NodeHost, workflowID string) *httptriggernode.HTTPTrigger {
	return httptriggernode.NewHTTPTrigger(stNode(id, "httpTrigger", workflowID), config, engine)
}

func NewInjectNode(id string, config *InjectConfig, engine nodebase.NodeHost, workflowID string) *injectnode.InjectNode {
	return injectnode.NewInjectNode(stNode(id, "inject", workflowID), config, engine)
}

func NewJSONNode(id string, config *JSONConfig) *jsonnode.JSONNode {
	return jsonnode.NewJSONNode(stNode(id, "json", ""), config)
}

func NewJoinNode(id string, config *JoinConfig, engine nodebase.NodeHost, workflowID string) *joinnode.JoinNode {
	return joinnode.NewJoinNode(stNode(id, "join", workflowID), config, engine)
}

func NewLinkCallNode(id string, config *LinkCallConfig, engine nodebase.NodeHost, workflowID string) *linkcallnode.LinkCallNode {
	return linkcallnode.NewLinkCallNode(stNode(id, "linkCall", workflowID), config, engine)
}

func NewMailOutNode(id string, config *MailOutConfig) *mailoutnode.MailOutNode {
	return mailoutnode.NewMailOutNode(stNode(id, "mailOut", ""), config)
}

func NewRBENode(id string, config *RBEConfig, engine nodebase.NodeHost, workflowID string) *rbencode.RBENode {
	return rbencode.NewRBENode(stNode(id, "rbe", workflowID), config, engine)
}

func NewRangeNode(id string, config *RangeConfig) *rangenode.RangeNode {
	return rangenode.NewRangeNode(stNode(id, "range", ""), config)
}

func NewSortNode(id string, config *SortConfig, engine nodebase.NodeHost, workflowID string) *sortnode.SortNode {
	return sortnode.NewSortNode(stNode(id, "sort", workflowID), config, engine)
}

func NewSplitNode(id string, config *SplitConfig) *splitnode.SplitNode {
	return splitnode.NewSplitNode(stNode(id, "split", ""), config)
}

func NewSubmitNode(id string, config *SubmitConfig, engine nodebase.NodeHost, workflowID string) *submitnode.SubmitNode {
	return submitnode.NewSubmitNode(stNode(id, "submitIn", workflowID), config, engine)
}

func NewSwitchNode(id string, config *SwitchConfig) *switchnode.SwitchNode {
	return switchnode.NewSwitchNode(stNode(id, "switch", ""), config)
}

func NewTCPRequestNode(id string, config *TCPRequestConfig, engine nodebase.NodeHost, workflowID string) *tcprequestnode.TCPRequestNode {
	return tcprequestnode.NewTCPRequestNode(stNode(id, "tcpRequest", workflowID), config, engine)
}

func NewTLSConfigNode(id string, config *TLSConfig) *tlsconfignode.TLSConfigNode {
	return tlsconfignode.NewTLSConfigNode(stNode(id, "tlsConfig", ""), config)
}

func NewTemplateNode(id string, config *TemplateConfig, engine nodebase.NodeHost) *templatenode.TemplateNode {
	return templatenode.NewTemplateNode(stNode(id, "template", ""), config, engine)
}

func NewTriggerNode(id string, config *TriggerConfig, engine nodebase.NodeHost, workflowID string) *triggernode.TriggerNode {
	return triggernode.NewTriggerNode(stNode(id, "trigger", workflowID), config, engine)
}

func NewXMLNode(id string, config *XMLConfig) *xmlnode.XMLNode {
	return xmlnode.NewXMLNode(stNode(id, "xml", ""), config)
}

// Contract type aliases so ported tests can use the flowrunner-era
// unqualified names (they lived in package flowrunner there).
type (
	Stopper         = nodebase.Stopper
	BaseNode        = nodebase.BaseNode
	Fs              = nodebase.Fs
	ContextStore    = nodebase.ContextStore
	QueueInspector  = nodebase.QueueInspector
	RunJournalEntry = nodebase.RunJournalEntry
	NodeExecutor    = nodebase.NodeExecutor
	NodeFactory     = nodebase.NodeFactory
)

type (
	QueueItem = nodebase.QueueItem
	HTMLNode  = htmlnode.HTMLNode
)

type FsEntry = nodebase.FsEntry

type ChangeRule = changenode.ChangeRule

// freePort reserves an ephemeral TCP port for test listeners (copy of
// the helper so internal-package tests can use it too).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// RegisterNodeType mirrors flowrunner's test-facing registration:
// the old factory shape took (Node, *Engine); it adapts to the new
// nodebase.NodeFactory contract.
func (e *Engine) RegisterNodeType(typeID string, factory func(Node, *Engine) (nodebase.NodeExecutor, error)) error {
	return e.WorkflowEngine.RegisterNodeType(typeID, func(node nodestore.NodeInterface, host nodebase.NodeHost) (nodebase.NodeExecutor, error) {
		n := Node{ID: node.GetID(), Type: node.GetType(), Data: node.GetData()}
		n.SetWorkflowID(node.GetWorkflowID())
		return factory(n, e)
	})
}

// NewBaseNode mirrors flowrunner.NewBaseNode(id, type).
func NewBaseNode(id, nodeType string) *nodebase.BaseNode {
	return nodebase.NewBaseNode(nodestore.NewNode(id, nodeType))
}

// nodeREDTabID mirrors flowrunner's deterministic module→tab id
// (fnv-1a 32 of the module name, "tab%08x").
func nodeREDTabID(module string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(module))
	return fmt.Sprintf("tab%08x", h.Sum32())
}

// RegisterNodeType mirrors flowrunner's package-level custom-type
// registration with the old (Node, *Engine) factory shape.
func RegisterNodeType(typeID string, factory func(Node, *Engine) (nodebase.NodeExecutor, error)) error {
	return nodebase.RegisterNodeType(typeID, func(node nodestore.NodeInterface, host nodebase.NodeHost) (nodebase.NodeExecutor, error) {
		n := Node{ID: node.GetID(), Type: node.GetType(), Data: node.GetData()}
		n.SetWorkflowID(node.GetWorkflowID())
		e, _ := host.(*WorkflowEngine)
		return factory(n, &Engine{WorkflowEngine: e})
	})
}

// FakeDurableStore is an in-memory DurableInboxStore recording every
// write/delete so tests can observe the write-through behaviour.
type FakeDurableStore struct {
	mu      sync.Mutex
	nextSeq int64
	rows    map[int64]DurableInboxItem // seq -> row, per workflow below
	byWf    map[string][]int64         // workflowID -> seqs
	saves   int
	deletes int
}

func NewFakeDurableStore() *FakeDurableStore {
	return &FakeDurableStore{
		nextSeq: 1,
		rows:    map[int64]DurableInboxItem{},
		byWf:    map[string][]int64{},
	}
}

func (s *FakeDurableStore) LoadInbox(workflowID string) ([]DurableInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DurableInboxItem
	for _, seq := range s.byWf[workflowID] {
		if row, ok := s.rows[seq]; ok {
			out = append(out, row)
		}
	}
	return out, nil
}

func (s *FakeDurableStore) SaveInboxItem(item *DurableInboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item.Seq = s.nextSeq
	s.nextSeq++
	row := *item
	s.rows[row.Seq] = row
	s.byWf[row.WorkflowID] = append(s.byWf[row.WorkflowID], row.Seq)
	s.saves++
	return nil
}

func (s *FakeDurableStore) DeleteInboxItem(workflowID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[seq]; !ok {
		return fmt.Errorf("row %d not found", seq)
	}
	delete(s.rows, seq)
	s.deletes++
	return nil
}

func (s *FakeDurableStore) DeleteWorkflowInbox(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range s.byWf[workflowID] {
		delete(s.rows, seq)
	}
	delete(s.byWf, workflowID)
	return nil
}

func (s *FakeDurableStore) RowCount(workflowID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, seq := range s.byWf[workflowID] {
		if _, ok := s.rows[seq]; ok {
			n++
		}
	}
	return n
}

// Seed writes a surviving row directly (a crashed runner's leftover).
func (s *FakeDurableStore) Seed(workflowID, nodeID string, data interface{}) {
	_ = s.SaveInboxItem(&DurableInboxItem{
		WorkflowID: workflowID,
		NodeID:     nodeID,
		Payload:    &Message{Payload: data, Timestamp: time.Now()},
		EnqueuedAt: time.Now(),
	})
}
