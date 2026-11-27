package noderunner

import (
	"encoding/json"
	"fmt"
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

// cloneWorkflow shallow-copies a workflow so the deploy-time component
// expansion can rewrite its node/edge lists without writing the
// expanded graph back into the caller's stored definition (flowrunner
// copied the Workflow struct value for this).
func cloneWorkflow(w nodestore.WorkflowInterface) nodestore.WorkflowInterface {
	c := nodestore.NewWorkflow(w.GetName()).
		SetID(w.GetID()).
		SetDescription(w.GetDescription()).
		SetDeployed(w.GetDeployed()).
		SetPaused(w.GetPaused()).
		SetCreatedAt(w.GetCreatedAt()).
		SetUpdatedAt(w.GetUpdatedAt())
	c.SetView(w.GetView())
	c.SetNodes(append([]nodestore.NodeInterface(nil), w.GetNodes()...))
	c.SetEdges(append([]nodestore.EdgeInterface(nil), w.GetEdges()...))
	return c
}

// scrubWorkflowCredentials returns a deep clone of w with every
// credential field re-masked to the sentinel. The running flow stored
// in e.flows carries injected secrets; observer/admin-facing copies
// (deploy observer, RunningFlow) must never expose them.
func scrubWorkflowCredentials(w nodestore.WorkflowInterface) nodestore.WorkflowInterface {
	clone := nodestore.DeepCloneWorkflow(w)
	for _, n := range clone.GetNodes() {
		data := n.GetData()
		for _, k := range nodestore.CredentialKeys(n.GetType()) {
			if _, ok := data[k]; ok {
				data[k] = nodestore.CredentialSentinel
			}
		}
	}
	return clone
}

// scrubNodeCredentials returns node unchanged when its type declares
// no credential keys, else a fresh node whose data clone has every
// credential field re-masked to the sentinel — the LookupNode view of
// a credential-injected running node.
func scrubNodeCredentials(node nodestore.NodeInterface) nodestore.NodeInterface {
	keys := nodestore.CredentialKeys(node.GetType())
	if len(keys) == 0 {
		return node
	}
	data := cloneDataMap(node.GetData())
	for _, k := range keys {
		if _, ok := data[k]; ok {
			data[k] = nodestore.CredentialSentinel
		}
	}
	return nodestore.NewNode(node.GetID(), node.GetType()).
		SetPosition(node.GetPositionX(), node.GetPositionY()).
		SetData(data)
}

// messageWire is the on-disk JSON shape of a message — identical to
// the legacy flowrunner.Message encoding, except payload/metadata use the
// shared durable payload JSON codec so []byte values survive as bytes.
// Durable queue files cannot marshal an interface field, so dirQueueFile
// stores this wire type and converts at the boundary.
type messageWire struct {
	ID        string          `json:"id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Error     *string         `json:"error,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	// LegacyData accepts the pre-Payload "data" key so queue files
	// written before the Data→Payload rename still decode (read-side
	// shim; writes always emit "payload").
	LegacyData json.RawMessage `json:"data,omitempty"`

	decodedPayload  any
	decodedMetadata map[string]any
}

// messageToWire converts a message into its JSON wire form.
func messageToWire(m nodestore.MessageInterface) (*messageWire, error) {
	if m == nil {
		return nil, nil
	}
	wire := &messageWire{
		ID:        m.GetID(),
		Error:     m.GetError(),
		Timestamp: m.GetTimestamp(),
	}
	if m.GetPayload() != nil {
		payload, err := nodestore.MarshalPayloadJSON(m.GetPayload())
		if err != nil {
			return nil, err
		}
		wire.Payload = json.RawMessage(payload)
	}
	if len(m.GetMetadata()) > 0 {
		metadata, err := nodestore.MarshalPayloadJSON(m.GetMetadata())
		if err != nil {
			return nil, err
		}
		wire.Metadata = json.RawMessage(metadata)
	}
	return wire, nil
}

// UnmarshalJSON decodes the wire form, including the shared binary payload
// contract, so a corrupt reserved binary object fails the queue-file read
// instead of silently changing type.
func (w *messageWire) UnmarshalJSON(raw []byte) error {
	type wireAlias messageWire
	var decoded wireAlias
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*w = messageWire(decoded)
	if len(w.Payload) > 0 {
		payload, err := nodestore.UnmarshalPayloadJSON(w.Payload)
		if err != nil {
			return err
		}
		w.decodedPayload = payload
	}
	if len(w.Metadata) > 0 {
		metadata, err := nodestore.UnmarshalPayloadJSON(w.Metadata)
		if err != nil {
			return err
		}
		metadataMap, ok := metadata.(map[string]any)
		if !ok {
			return fmt.Errorf("queue message metadata is not a JSON object")
		}
		w.decodedMetadata = metadataMap
	}
	return nil
}

// toMessage converts a decoded wire form back into a message,
// applying the legacy "data" payload shim.
func (w *messageWire) toMessage() nodestore.MessageInterface {
	m := nodestore.NewMessage()
	if w == nil {
		return m
	}
	m.SetID(w.ID)
	m.SetPayload(w.decodedPayload)
	m.SetMetadata(w.decodedMetadata)
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
