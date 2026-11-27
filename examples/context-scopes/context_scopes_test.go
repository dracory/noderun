package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestRunPrintsScopeVisibility(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	runErr := run()
	_ = writer.Close()
	os.Stdout = original

	output, _ := io.ReadAll(reader)
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
	if !bytes.Contains(output, []byte(`{"nodeSees":null,"flowSees":"flow value","globalSees":"GoFlow"}`)) {
		t.Fatalf("expected scope report in output, got: %s", output)
	}
}
