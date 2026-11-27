package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestRunReleasesParkedMessage(t *testing.T) {
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
	if !bytes.Contains(output, []byte("the parked message is free!")) {
		t.Fatalf("expected released message in output, got: %s", output)
	}
}
