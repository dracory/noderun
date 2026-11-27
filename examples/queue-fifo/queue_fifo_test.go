package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestRunDrainsInFIFOOrder(t *testing.T) {
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
	// FIFO order must be preserved on release.
	if !bytes.Contains(output, []byte("1,2,3")) {
		t.Fatalf("expected FIFO drain order 1,2,3, got: %s", output)
	}
}
