package preflight

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/baizeai/kcover/pkg/events"
)

func TestLoadReportFile(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "default")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	reportPath := filepath.Join(path, "worker-0.json")
	if err := os.WriteFile(reportPath, []byte(`{"version":1,"result":2,"node_name":"node-a","check":{"gpu":1,"nic":1,"storage":1,"node_check":1,"network":{"result":2,"target":{"node-b":2}}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	report, err := LoadReportFile(baseDir, "default", "worker-0")
	if err != nil {
		t.Fatalf("LoadReportFile() error = %v", err)
	}

	if report.NodeName != "node-a" {
		t.Fatalf("report.NodeName = %q, want %q", report.NodeName, "node-a")
	}
	if report.Checks.Network.Target["node-b"] != CheckResultFail {
		t.Fatalf("report.Checks.Network.Target[node-b] = %v, want %v", report.Checks.Network.Target["node-b"], CheckResultFail)
	}
}

func TestNodeEvents(t *testing.T) {
	t.Parallel()

	collected := NodeEvents("default", "worker-0", Report{
		NodeName: "node-a",
		Checks: Check{
			Network: Network{Target: map[string]CheckResult{
				"node-c": CheckResultPass,
				"node-b": CheckResultFail,
			}},
		},
	})

	if len(collected) != 2 {
		t.Fatalf("len(NodeEvents(...)) = %d, want 2", len(collected))
	}

	want := []events.Event{
		{ResourceType: events.Node, Name: "node-a", EventType: events.Error, Message: "pod default/worker-0 preflight failed on node node-a"},
		{ResourceType: events.Node, Name: "node-b", EventType: events.Error, Message: "pod default/worker-0 preflight reported network failure to node node-b"},
	}
	if !reflect.DeepEqual(collected, want) {
		t.Fatalf("NodeEvents(...) = %#v, want %#v", collected, want)
	}
}

func TestReportPath(t *testing.T) {
	t.Parallel()

	path := ReportPath("/var/lib/kcover/preflight", "default", "worker-0")
	if path != "/var/lib/kcover/preflight/default/worker-0.json" {
		t.Fatalf("ReportPath(...) = %q, want %q", path, "/var/lib/kcover/preflight/default/worker-0.json")
	}
}
