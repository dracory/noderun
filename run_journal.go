package noderunner

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"time"
)

// RunJournalSize bounds how many execution records the engine keeps
// per (workflow, node). 20 is a deliberately small ring: enough to see
// the recent behaviour of a node during a debug session (FastGPT-style
// "run details"), small enough that a 1000-node flow journaling every
// execution stays in the tens of megabytes worst-case (each stored
// payload summary is itself bounded by MaxJournalPayloadBytes).
// The constant lives in nodes (the runner-agnostic contract package)
// so the admin API can report the bound without importing noderunner.
const RunJournalSize = nodebase.RunJournalSize

// MaxJournalPayloadBytes bounds the JSON-serialized size of each
// summarized input/output stored in a journal entry. Larger payloads
// are replaced by a {truncated, bytes, preview} marker so a fat
// message (binary file, bulk row set) cannot bloat the ring — 4 KiB
// keeps one entry comfortably inspectable in the editor while the
// worst-case memory of a full 20-entry ring stays under ~80 KiB/node.
const MaxJournalPayloadBytes = 4096

// JournalMasked replaces credential-keyed values in journaled payload
// summaries — the journal is inspectable via the admin API, so secrets
// must never be stored in it.
const JournalMasked = "********"

// MaxJournalScrubDepth bounds the recursion of the credential scrub so
// a pathological nested payload cannot stack-overflow the summarizer.
const MaxJournalScrubDepth = 8

// The journal entry type is nodebase.RunJournalEntry — the
// runner-agnostic contract the admin surface is written against (see
// nodes/inspection.go).

// recordRun appends one execution to the node's bounded journal ring.
// The summaries are built before the lock — scrub + marshal cost stays
// off runMu. Called once per runNode execution.
func (e *WorkflowEngine) recordRun(workflowID string, node nodestore.NodeInterface, in, out nodestore.MessageInterface, dur time.Duration, runErr error) {
	entry := nodebase.RunJournalEntry{
		Time:       time.Now(),
		DurationMs: float64(dur) / float64(time.Millisecond),
		Input:      summarizeJournalPayload(node.GetType(), in),
	}
	if runErr != nil {
		entry.Error = maskURICredentials(runErr.Error())
	} else {
		entry.Output = summarizeJournalPayload(node.GetType(), out)
	}

	e.runMu.Lock()
	defer e.runMu.Unlock()
	e.seq++
	entry.Seq = e.seq
	journal := e.journal[workflowID]
	if journal == nil {
		journal = make(map[string][]nodebase.RunJournalEntry)
		e.journal[workflowID] = journal
	}
	entries := append(journal[node.GetID()], entry)
	if len(entries) > RunJournalSize {
		entries = entries[len(entries)-RunJournalSize:]
	}
	journal[node.GetID()] = entries
}

// RunJournal returns a copy of the node's recorded execution journal
// (oldest first). ok is false when the workflow is not running — the
// journal is run-scoped state like node statuses and is dropped when
// the workflow stops or redeploys.
func (e *WorkflowEngine) RunJournal(workflowID, nodeID string) ([]nodebase.RunJournalEntry, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if _, running := e.flows[workflowID]; !running {
		return nil, false
	}
	entries := e.journal[workflowID][nodeID]
	out := make([]nodebase.RunJournalEntry, len(entries))
	copy(out, entries)
	return out, true
}

// uriCredPattern matches credentials embedded in URIs
// (scheme://user:pass@host) — executor error strings can echo them
// (dial errors, DSNs), so event/journal/problem text is masked before
// it reaches durable or admin-readable surfaces.
var uriCredPattern = regexp.MustCompile(`(://[^:/@\s]+:)[^@\s]+(@)`)

// maskURICredentials replaces the password component of embedded URI
// credentials with the journal mask; safe on any string.
func maskURICredentials(s string) string {
	return uriCredPattern.ReplaceAllString(s, "${1}"+JournalMasked+"${2}")
}

// summarizeJournalPayload builds a bounded, credential-safe JSON-able
// copy of a payload for the journal. The result is a small map with
// the payload's data/metadata/error — scrubbed so keys listed as
// credentials for the executing node's type (nodestore.IsCredentialKey) and any
// nodestore.CredentialSentinel value are masked, then size-bounded; oversized
// payloads degrade to a {truncated, bytes, preview} marker.
func summarizeJournalPayload(nodeType string, p nodestore.MessageInterface) interface{} {
	if p == nil {
		return nil
	}
	summary := map[string]interface{}{}
	if p.GetPayload() != nil {
		if b, ok := p.GetPayload().([]byte); ok {
			// Binary bodies (fileIn, submitIn uploads) journal as a
			// byte count — base64-encoding them would defeat the size
			// bound and leak file content into the ring.
			summary["data"] = fmt.Sprintf("<binary: %d bytes>", len(b))
		} else {
			summary["data"] = scrubJournalValue(nodeType, p.GetPayload(), 0)
		}
	}
	if len(p.GetMetadata()) > 0 {
		summary["metadata"] = scrubJournalValue(nodeType, p.GetMetadata(), 0)
	}
	if p.GetError() != nil {
		summary["error"] = *p.GetError()
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return map[string]interface{}{"unmarshalable": true}
	}
	if len(raw) <= MaxJournalPayloadBytes {
		return summary
	}
	return map[string]interface{}{
		"truncated": true,
		"bytes":     len(raw),
		"preview":   string(raw[:MaxJournalPayloadBytes]),
	}
}

// scrubJournalValue returns a deep copy of v safe for the journal:
// map keys that are credential fields for nodeType (or hold the
// credential sentinel) are masked at every level. Values that are not
// JSON-shaped (channels, functions) degrade to their %v string.
func scrubJournalValue(nodeType string, v interface{}, depth int) interface{} {
	if depth > MaxJournalScrubDepth {
		return "<depth limit>"
	}
	switch t := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			if nodestore.IsCredentialKey(nodeType, k) || nodestore.IsCredentialSentinel(val) {
				out[k] = JournalMasked
				continue
			}
			out[k] = scrubJournalValue(nodeType, val, depth+1)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			out[i] = scrubJournalValue(nodeType, val, depth+1)
		}
		return out
	case []byte:
		return fmt.Sprintf("<binary: %d bytes>", len(t))
	default:
		if nodestore.IsCredentialSentinel(v) {
			return JournalMasked
		}
		// Keep plain JSON scalars as-is; anything else (channels,
		// funcs, unmarshalable values) would fail json.Marshal, so
		// degrade it to a string up front.
		switch v.(type) {
		case nil, bool, string, float64, float32,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			json.Number:
			return v
		}
		return fmt.Sprintf("%v", v)
	}
}
