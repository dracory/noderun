package noderunner_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dracory/nodebase/compress"
	nodestore "github.com/dracory/nodestore"
)

// TestCompressOutputDurableRoundTrip proves compressed binary output
// survives nodestore/entitystore persistence and still decompresses to
// the original bytes after recovery.
func TestCompressOutputDurableRoundTrip(t *testing.T) {
	ctx := context.Background()
	original := []byte{0, 1, 2, 0xff, 'c', 'o', 'm', 'p', 'r', 'e', 's', 's'}
	node := compress.NewNode(nodestore.NewNode("zip", "compress"), &compress.Config{
		Operation: "compress", Algorithm: "gzip", Property: "payload",
	})
	compressedMsg, err := node.Execute(ctx, nodestore.NewMessage().SetPayload(original))
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if _, ok := compressedMsg.GetPayload().([]byte); !ok {
		t.Fatalf("compressed payload is %T", compressedMsg.GetPayload())
	}

	store := resumeTestStore(t)
	if err := store.MessageCreate(ctx, compressedMsg); err != nil {
		t.Fatalf("entitystore create: %v", err)
	}
	recovered, err := store.MessageFindByID(ctx, compressedMsg.GetID())
	if err != nil {
		t.Fatalf("entitystore find: %v", err)
	}
	if _, ok := recovered.GetPayload().([]byte); !ok {
		t.Fatalf("recovered compressed payload is %T", recovered.GetPayload())
	}

	unzip := compress.NewNode(nodestore.NewNode("unzip", "compress"), &compress.Config{
		Operation: "decompress", Algorithm: "gzip", Property: "payload",
	})
	roundTripped, err := unzip.Execute(ctx, recovered)
	if err != nil {
		t.Fatalf("decompress after durable round-trip: %v", err)
	}
	got, ok := roundTripped.GetPayload().([]byte)
	if !ok || !bytes.Equal(got, original) {
		t.Fatalf("decompressed payload = %#v (%T), want original bytes", roundTripped.GetPayload(), roundTripped.GetPayload())
	}
}
