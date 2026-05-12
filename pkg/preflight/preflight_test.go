package preflight

import (
	"reflect"
	"sort"
	"testing"
)

func TestParseReportSupportsTargets(t *testing.T) {
	t.Parallel()

	report, err := parseReport(`{"version":1,"workload":"resnet50","world_size":4,"rank":0,"result":2,"node_name":"node-a","check":{"storage":1,"gpu":1,"node_check":0,"nic":1,"network":{"result":2,"target":{"node-b":2,"node-c":1}}}}`)
	if err != nil {
		t.Fatalf("ParseReport returned error: %v", err)
	}

	if report.Result != CheckResultFail {
		t.Fatalf("report.Result = %v, want %v", report.Result, CheckResultFail)
	}
	if report.Checks.NodeCheck != CheckResultSkip {
		t.Fatalf("report.Checks.NodeCheck = %v, want %v", report.Checks.NodeCheck, CheckResultSkip)
	}
	if !reflect.DeepEqual(failedTargets(report), []string{"node-b"}) {
		t.Fatalf("failedTargets(report) = %v, want [node-b]", failedTargets(report))
	}
}

func TestParseReportRejectsMissingNodeName(t *testing.T) {
	t.Parallel()

	if _, err := parseReport(`{"version":1,"result":2,"check":{"network":{"result":2,"target":{"node-b":2}}}}`); err == nil {
		t.Fatal("ParseReport error = nil, want non-nil when node_name is missing")
	}
}

func TestPreflightReportPassWhenAllReportsPass(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultPass, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass}),
		reportJSON("node-b", CheckResultPass, CheckResultPass, map[string]CheckResult{"node-a": CheckResultPass}),
	}); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	result, badNodes, err := p.Report()
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if result != CheckResultPass {
		t.Fatalf("Analyze result = %v, want %v", result, CheckResultPass)
	}
	if len(badNodes) != 0 {
		t.Fatalf("Report badNodes = %v, want []", badNodes)
	}
}

func TestPreflightReportCombinesLocalAndFilteredNetworkFailures(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass, "node-c": CheckResultPass, "node-d": CheckResultPass}),
		reportJSON("node-b", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultPass, "node-c": CheckResultFail, "node-d": CheckResultFail}),
		reportJSON("node-c", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultPass, "node-b": CheckResultFail, "node-d": CheckResultPass}),
		reportJSON("node-d", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultPass, "node-b": CheckResultFail, "node-c": CheckResultPass}),
	}); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	result, badNodes, err := p.Report()
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if result != CheckResultFail {
		t.Fatalf("Report result = %v, want %v", result, CheckResultFail)
	}
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b"}) {
		t.Fatalf("Report badNodes = %v, want [node-a node-b]", badNodes)
	}
}

func TestParseResetsExistingReports(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{reportJSON("node-a", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass})}); err != nil {
		t.Fatalf("first Parse returned error: %v", err)
	}
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultPass, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass}),
		reportJSON("node-b", CheckResultPass, CheckResultPass, map[string]CheckResult{"node-a": CheckResultPass}),
	}); err != nil {
		t.Fatalf("second Parse returned error: %v", err)
	}

	result, badNodes, err := p.Report()
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if result != CheckResultPass || len(badNodes) != 0 {
		t.Fatalf("Report = (%v, %v), want (pass, [])", result, badNodes)
	}
}

func TestPreflightReportFiltersLocalBadNodesBeforeRemainingFailures(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass, "node-c": CheckResultPass, "node-d": CheckResultPass}),
		reportJSON("node-b", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-c": CheckResultFail, "node-d": CheckResultFail}),
		reportJSON("node-c", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-b": CheckResultFail, "node-d": CheckResultPass}),
		reportJSON("node-d", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-b": CheckResultFail, "node-c": CheckResultPass}),
	}); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	result, badNodes, err := p.Report()
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if result != CheckResultFail {
		t.Fatalf("Report result = %v, want %v", result, CheckResultFail)
	}
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b"}) {
		t.Fatalf("Report badNodes = %v, want [node-a node-b]", badNodes)
	}

	filtered := filterReports(p.reports, map[string]struct{}{"node-a": {}})
	if len(filtered) != 3 {
		t.Fatalf("len(filterReports(...)) = %d, want 3", len(filtered))
	}
	if _, exists := filtered[0].Checks.Network.Target["node-a"]; exists {
		t.Fatalf("node-a target still exists after filtering: %v", filtered[0].Checks.Network.Target)
	}
	if filtered[0].WorldSize != 3 {
		t.Fatalf("filtered report world_size = %d, want 3", filtered[0].WorldSize)
	}
}

func reportJSON(nodeName string, result CheckResult, networkResult CheckResult, targets map[string]CheckResult) string {
	return `{"version":1,"workload":"resnet50","world_size":` +
		itoa(len(targets)+1) +
		`,"node_name":"` + nodeName + `"` +
		`,"result":` + itoa(int(result)) +
		`,"check":{"storage":0,"gpu":1,"node_check":1,"nic":1,"network":{"result":` + itoa(int(networkResult)) + `,"target":` + targetsJSON(targets) + `}}}`
}

func targetsJSON(targets map[string]CheckResult) string {
	keys := make([]string, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	json := "{"
	for i, key := range keys {
		if i > 0 {
			json += ","
		}
		json += `"` + key + `":` + itoa(int(targets[key]))
	}
	json += "}"

	return json
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}

	buf := [20]byte{}
	index := len(buf)
	for value > 0 {
		index--
		buf[index] = byte('0' + value%10)
		value /= 10
	}

	return string(buf[index:])
}

func failedTargets(report *Report) []string {
	targets := make([]string, 0)
	for nodeName, result := range report.Checks.Network.Target {
		if result == CheckResultFail {
			targets = append(targets, nodeName)
		}
	}
	sort.Strings(targets)

	return targets
}
