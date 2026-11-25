package noderunner

import (
	"context"
	"encoding/base64"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fileExecutor builds a node executor via the engine's executorForNode
// path (same wiring as production).
func fileExecutor(t *testing.T, engine *Engine, nodeType string, data map[string]interface{}) NodeExecutor {
	t.Helper()
	if data == nil {
		data = map[string]interface{}{}
	}
	executor, err := engine.executorForNode("wf", nodestore.NewNode("n1", nodeType).SetData(data))
	if err != nil {
		t.Fatalf("executorForNode(%s) failed: %v", nodeType, err)
	}
	return executor
}

// TestFileNodeWriteReadRoundTrip writes a payload via the file node and
// reads it back via fileIn (utf8).
func TestFileNodeWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir

	target := filepath.Join(dir, "out.txt")
	writer := fileExecutor(t, engine, "file", map[string]interface{}{
		"filename":      target,
		"createDir":     true,
		"appendNewline": false,
	})
	in := &Message{Payload: "hello world", Metadata: map[string]interface{}{}}
	out, err := writer.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("file write failed: %v", err)
	}
	if out.GetMetadata()["filename"] != target {
		t.Fatalf("expected filename metadata %q, got %v", target, out.GetMetadata()["filename"])
	}

	reader := fileExecutor(t, engine, "fileIn", map[string]interface{}{
		"filename": target,
		"format":   "utf8",
	})
	read, err := reader.Execute(context.Background(), &Message{Metadata: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("fileIn read failed: %v", err)
	}
	if read.GetPayload() != "hello world" {
		t.Fatalf("expected %q, got %v", "hello world", read.GetPayload())
	}
	if read.GetMetadata()["filename"] != target {
		t.Fatalf("expected filename metadata preserved, got %v", read.GetMetadata()["filename"])
	}
}

// TestFileNodeAppendAndNewline verifies overwriteFile=false appends and
// appendNewline adds a trailing newline per write.
func TestFileNodeAppendAndNewline(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	target := filepath.Join(dir, "log.txt")

	writer := fileExecutor(t, engine, "file", map[string]interface{}{
		"filename":      target,
		"appendNewline": true,
		"overwriteFile": "false",
	})
	for _, data := range []string{"one", "two"} {
		if _, err := writer.Execute(context.Background(), &Message{Payload: data}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "one\ntwo\n" {
		t.Fatalf("unexpected file content %q", raw)
	}
}

// TestFileInLines verifies the "lines" format emits one message per line
// tagged with parts metadata.
func TestFileInLines(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	target := filepath.Join(dir, "lines.txt")
	if err := os.WriteFile(target, []byte("a\nb\nc\n"), 0644); err != nil {
		t.Fatal(err)
	}

	exec := fileExecutor(t, engine, "fileIn", map[string]interface{}{
		"filename": target,
		"format":   "lines",
	})
	out, err := exec.Execute(context.Background(), &Message{Metadata: map[string]interface{}{"keep": "x"}})
	if err != nil {
		t.Fatalf("fileIn failed: %v", err)
	}
	expander, ok := exec.(nodebase.MessageExpander)
	if !ok {
		t.Fatal("fileIn does not implement nodebase.MessageExpander")
	}
	msgs := expander.Expand(out)
	// Node-RED fileIn lines mode emits the trailing empty segment produced
	// by a final newline, and tags the sequence as type "string" so join
	// rebuilds text with parts.ch. (nodebase fileIn parity change.)
	wantLines := []string{"a", "b", "c", ""}
	if len(msgs) != len(wantLines) {
		t.Fatalf("expected %d messages, got %d", len(wantLines), len(msgs))
	}
	for i, m := range msgs {
		if m.GetPayload() != wantLines[i] {
			t.Fatalf("msg %d: expected %q, got %v", i, wantLines[i], m.GetPayload())
		}
		parts, ok := m.GetMetadata()["parts"].(map[string]interface{})
		if !ok {
			t.Fatalf("msg %d missing parts metadata", i)
		}
		if parts["index"] != i || parts["count"] != len(wantLines) || parts["type"] != "string" || parts["ch"] != "\n" {
			t.Fatalf("msg %d bad parts: %v", i, parts)
		}
		if m.GetMetadata()["filename"] != target {
			t.Fatalf("msg %d missing filename metadata", i)
		}
	}
}

// TestFileNodePathEscape verifies a ../ escape is rejected when FileRoot
// is configured.
func TestFileNodePathEscape(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir

	evil := filepath.Join(dir, "..", "..", "etc", "passwd")
	for _, nodeType := range []string{"file", "fileIn"} {
		exec := fileExecutor(t, engine, nodeType, map[string]interface{}{
			"filename": evil,
		})
		_, err := exec.Execute(context.Background(), &Message{Payload: "x", Metadata: map[string]interface{}{}})
		if err == nil || !strings.Contains(err.Error(), "escapes file root") {
			t.Fatalf("%s: expected escape rejection, got %v", nodeType, err)
		}
	}

	// Without FileRoot the same path passes the sandbox check
	engine.FileRoot = ""
	if _, err := nodebase.ResolveFilePath("", evil); err != nil {
		t.Fatalf("unrestricted path rejected: %v", err)
	}
}

// TestFileNodeBase64Encoding verifies base64 payload decoding on write.
func TestFileNodeBase64Encoding(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	target := filepath.Join(dir, "bin.dat")

	writer := fileExecutor(t, engine, "file", map[string]interface{}{
		"filename": target,
		"encoding": "base64",
	})
	_, err := writer.Execute(context.Background(), &Message{
		Payload: base64.StdEncoding.EncodeToString([]byte("binary!")),
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "binary!" {
		t.Fatalf("unexpected content %q", raw)
	}
}

// TestFileNodeFilenameFromMsg verifies filenameType "msg" resolves the
// filename from message data/metadata.
func TestFileNodeFilenameFromMsg(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	target := filepath.Join(dir, "frommsg.txt")

	writer := fileExecutor(t, engine, "file", map[string]interface{}{
		"filenameType": "msg",
	})
	out, err := writer.Execute(context.Background(), &Message{
		Payload:  "via msg",
		Metadata: map[string]interface{}{"filename": target},
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if out.GetMetadata()["filename"] != target {
		t.Fatalf("expected %q, got %v", target, out.GetMetadata()["filename"])
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "via msg" {
		t.Fatalf("unexpected content %q", raw)
	}
}

// TestFileInSendError verifies a missing file emits an error message
// instead of a catchable error when sendError is set.
func TestFileInSendError(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	missing := filepath.Join(dir, "nope.txt")

	exec := fileExecutor(t, engine, "fileIn", map[string]interface{}{
		"filename":  missing,
		"sendError": true,
	})
	out, err := exec.Execute(context.Background(), &Message{Metadata: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("sendError should not error, got %v", err)
	}
	m, ok := out.GetPayload().(map[string]interface{})
	if !ok || m["error"] == nil {
		t.Fatalf("expected error message in Data, got %v", out.GetPayload())
	}

	// Without sendError the same read returns an error
	exec = fileExecutor(t, engine, "fileIn", map[string]interface{}{
		"filename": missing,
	})
	if _, err := exec.Execute(context.Background(), &Message{Metadata: map[string]interface{}{}}); err == nil {
		t.Fatal("expected catchable error for missing file")
	}
}

// TestFileInBinary verifies the binary format returns raw bytes.
func TestFileInBinary(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine()
	engine.FileRoot = dir
	target := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(target, []byte{0, 1, 2, 255}, 0644); err != nil {
		t.Fatal(err)
	}
	exec := fileExecutor(t, engine, "fileIn", map[string]interface{}{
		"filename": target,
		"format":   "binary",
	})
	out, err := exec.Execute(context.Background(), &Message{Metadata: map[string]interface{}{}})
	if err != nil {
		t.Fatal(err)
	}
	b, ok := out.GetPayload().([]byte)
	if !ok || len(b) != 4 || b[3] != 255 {
		t.Fatalf("expected raw bytes, got %v", out.GetPayload())
	}
}
