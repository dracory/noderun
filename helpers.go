package noderunner

import (
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	"time"
)

// Engine-side helpers added during the flowrunner port: the
// interface-first domain model has no struct literals, so a few
// mechanical conversions need small glue functions.

// messageWithError sets a *string error on a message built via the
// fluent nodestore.NewMessage() chain (used where flowrunner had a Message{...}
// literal with an Error field).
func messageWithError(m nodestore.MessageInterface, err *string) nodestore.MessageInterface {
	if err != nil {
		m.SetError(*err)
	}
	return m
}

// cloneWorkflow shallow-copies a workflow so the deploy-time subflow
// expansion can rewrite its node/edge lists without writing the
// expanded graph back into the caller's stored definition (flowrunner
// copied the Workflow struct value for this).
func cloneWorkflow(w nodestore.WorkflowInterface) nodestore.WorkflowInterface {
	c := nodestore.NewWorkflow(w.GetName()).
		SetID(w.GetID()).
		SetDescription(w.GetDescription()).
		SetDeployed(w.GetDeployed()).
		SetCreatedAt(w.GetCreatedAt()).
		SetUpdatedAt(w.GetUpdatedAt())
	c.SetView(w.GetView())
	c.SetNodes(append([]nodestore.NodeInterface(nil), w.GetNodes()...))
	c.SetEdges(append([]nodestore.EdgeInterface(nil), w.GetEdges()...))
	return c
}

// messageWire is the on-disk JSON shape of a message — identical to
// the legacy flowrunner.Message encoding. Durable queue files cannot
// marshal an interface field, so dirQueueFile stores this wire type
// and converts at the boundary.
type messageWire struct {
	ID        string         `json:"id,omitempty"`
	Payload   any            `json:"payload,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Error     *string        `json:"error,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	// LegacyData accepts the pre-Payload "data" key so queue files
	// written before the Data→Payload rename still decode (read-side
	// shim; writes always emit "payload").
	LegacyData json.RawMessage `json:"data,omitempty"`
}

// messageToWire converts a message into its JSON wire form.
func messageToWire(m nodestore.MessageInterface) *messageWire {
	if m == nil {
		return nil
	}
	return &messageWire{
		ID:        m.GetID(),
		Payload:   m.GetPayload(),
		Metadata:  m.GetMetadata(),
		Error:     m.GetError(),
		Timestamp: m.GetTimestamp(),
	}
}

// toMessage converts a decoded wire form back into a message,
// applying the legacy "data" payload shim.
func (w *messageWire) toMessage() nodestore.MessageInterface {
	m := nodestore.NewMessage()
	if w == nil {
		return m
	}
	m.SetID(w.ID)
	m.SetPayload(w.Payload)
	m.SetMetadata(w.Metadata)
	if w.Error != nil {
		m.SetError(*w.Error)
	}
	m.SetTimestamp(w.Timestamp)
	if m.GetPayload() == nil && len(w.LegacyData) > 0 {
		var payload any
		if json.Unmarshal(w.LegacyData, &payload) == nil {
			m.SetPayload(payload)
		}
	}
	return m
}
