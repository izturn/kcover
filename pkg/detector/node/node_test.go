package node

import (
	"testing"

	kcoverconfig "github.com/baizeai/kcover/cmd/agent/config"
	"github.com/baizeai/kcover/pkg/events"
)

type stubSink struct{}

func (stubSink) RecordEvent(events.Event) error {
	return nil
}

func TestNewDetectorRejectsNilSink(t *testing.T) {
	t.Parallel()

	if _, err := NewDetector("node-a", MetaX, 5, kcoverconfig.MetaX{}, nil); err == nil {
		t.Fatal("NewDetector error = nil, want non-nil for nil sink")
	}
}

func TestNewDetectorRejectsUnknownVendor(t *testing.T) {
	t.Parallel()

	if _, err := NewDetector("node-a", Vendor(99), 5, kcoverconfig.MetaX{}, stubSink{}); err == nil {
		t.Fatal("NewDetector error = nil, want non-nil for unknown vendor")
	}
}

func TestNewDetectorReturnsRunner(t *testing.T) {
	t.Parallel()

	detector, err := NewDetector("node-a", Nvidia, 5, kcoverconfig.MetaX{}, stubSink{})
	if err != nil {
		t.Fatalf("NewDetector returned error: %v", err)
	}
	if detector == nil {
		t.Fatal("NewDetector result = nil, want non-nil")
	}
}
