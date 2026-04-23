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
	targets := failedTargets(report)
	if !reflect.DeepEqual(targets, []string{"node-b"}) {
		t.Fatalf("failedTargets(report) = %v, want [node-b]", targets)
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

func TestPreflightReportCombinesLocalAndNetworkFailures(t *testing.T) {
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
		t.Fatalf("Analyze result = %v, want %v", result, CheckResultFail)
	}
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b"}) {
		t.Fatalf("Report badNodes = %v, want [node-a node-b]", badNodes)
	}
}

func TestPreflightReportReturnsAllLocalFailuresWithoutNetworkDiagnosis(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass}),
		reportJSON("node-b", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-a": CheckResultPass}),
	}); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	result, badNodes, err := p.Report()
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if result != CheckResultFail {
		t.Fatalf("Analyze result = %v, want %v", result, CheckResultFail)
	}
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b"}) {
		t.Fatalf("Report badNodes = %v, want [node-a node-b]", badNodes)
	}
}

func TestPreflightReportTreatsAllFailedTargetsAsLocalBadNode(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-b": CheckResultFail}),
		reportJSON("node-b", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail}),
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

func TestPreflightReportTreatsAllFilteredFailedTargetsAsLocalBadNode(t *testing.T) {
	t.Parallel()

	p := New()
	if err := p.Parse([]string{
		reportJSON("node-a", CheckResultFail, CheckResultPass, map[string]CheckResult{"node-b": CheckResultPass, "node-c": CheckResultPass}),
		reportJSON("node-b", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-c": CheckResultFail}),
		reportJSON("node-c", CheckResultFail, CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-b": CheckResultFail}),
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
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("Report badNodes = %v, want [node-a node-b node-c]", badNodes)
	}
	filtered := filterReports(p.reports, map[string]struct{}{"node-a": {}})
	if !reflect.DeepEqual(nodesWithAllFailedTargets(filtered), []string{"node-b", "node-c"}) {
		t.Fatalf("nodesWithAllFailedTargets(filtered) = %v, want [node-b node-c]", nodesWithAllFailedTargets(filtered))
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

func TestPreflightReportFiltersLocalBadNodesBeforeNetworkDiagnosis(t *testing.T) {
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

func TestAnalyzeReportsLargeComponent(t *testing.T) {
	t.Parallel()

	const worldSize = 17
	reports := make([]*Report, 0, worldSize)
	center := "node-00"
	for i := 0; i < worldSize; i++ {
		nodeName := nodeID(i)
		targets := make(map[string]CheckResult, worldSize-1)
		for j := 0; j < worldSize; j++ {
			if i == j {
				continue
			}
			peer := nodeID(j)
			result := CheckResultPass
			if nodeName == center || peer == center {
				result = CheckResultFail
			}
			targets[peer] = result
		}
		reports = append(reports, newReport("resnet50", worldSize, nodeName, CheckResultFail, targets))
	}

	diagnosis, err := diagnoseNetwork(reports)
	if err != nil {
		t.Fatalf("diagnoseNetwork returned error: %v", err)
	}
	if !reflect.DeepEqual(diagnosis.badNodes, []string{center}) {
		t.Fatalf("badNodes = %v, want [%s]", diagnosis.badNodes, center)
	}
	if len(diagnosis.suspectNodes) != 0 {
		t.Fatalf("suspectNodes = %v, want []", diagnosis.suspectNodes)
	}
}

func TestAnalyzeReportsFindsSuspectTriangle(t *testing.T) {
	t.Parallel()

	diagnosis, err := diagnoseNetwork([]*Report{
		newReport("resnet50", 3, "node-a", CheckResultFail, map[string]CheckResult{"node-b": CheckResultFail, "node-c": CheckResultFail}),
		newReport("resnet50", 3, "node-b", CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-c": CheckResultFail}),
		newReport("resnet50", 3, "node-c", CheckResultFail, map[string]CheckResult{"node-a": CheckResultFail, "node-b": CheckResultFail}),
	})
	if err != nil {
		t.Fatalf("diagnoseNetwork returned error: %v", err)
	}
	if len(diagnosis.badNodes) != 0 {
		t.Fatalf("badNodes = %v, want []", diagnosis.badNodes)
	}
	if !reflect.DeepEqual(diagnosis.suspectNodes, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("suspectNodes = %v, want [node-a node-b node-c]", diagnosis.suspectNodes)
	}
}

func TestSolveComponentExactlyLargePathMatchesEnumeration(t *testing.T) {
	t.Parallel()

	component := graphComponent{
		nodes: make([]string, 17),
		edges: make([]graphEdge, 0, 16),
	}
	for i := range component.nodes {
		component.nodes[i] = nodeID(i)
		if i > 0 {
			component.edges = append(component.edges, graphEdge{U: i - 1, V: i})
		}
	}

	wantRequired, wantOptional := solveByEnumeration(component)
	gotRequired, gotOptional := solveComponentExactly(component)

	if !reflect.DeepEqual(gotRequired, wantRequired) {
		t.Fatalf("required = %v, want %v", gotRequired, wantRequired)
	}
	if !reflect.DeepEqual(gotOptional, wantOptional) {
		t.Fatalf("optional = %v, want %v", gotOptional, wantOptional)
	}
}

func TestSolveByBranchAndBoundMatchesEnumerationOnAllGraphsUpToSixNodes(t *testing.T) {
	t.Parallel()

	for nodeCount := 2; nodeCount <= 6; nodeCount++ {
		edgeCount := nodeCount * (nodeCount - 1) / 2
		limit := uint64(1) << edgeCount
		for edgeMask := uint64(0); edgeMask < limit; edgeMask++ {
			component := componentFromEdgeMask(nodeCount, edgeMask)

			wantRequired, wantOptional := solveByEnumeration(component)
			gotRequired, gotOptional := solveByBranchAndBound(component)

			if !reflect.DeepEqual(gotRequired, wantRequired) {
				t.Fatalf("nodeCount=%d edgeMask=%b required = %v, want %v", nodeCount, edgeMask, gotRequired, wantRequired)
			}
			if !reflect.DeepEqual(gotOptional, wantOptional) {
				t.Fatalf("nodeCount=%d edgeMask=%b optional = %v, want %v", nodeCount, edgeMask, gotOptional, wantOptional)
			}
		}
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

func nodeID(i int) string {
	if i < 10 {
		return "node-0" + itoa(i)
	}
	return "node-" + itoa(i)
}

func componentFromEdgeMask(nodeCount int, edgeMask uint64) graphComponent {
	component := graphComponent{
		nodes: make([]string, nodeCount),
		edges: make([]graphEdge, 0),
	}
	for i := 0; i < nodeCount; i++ {
		component.nodes[i] = nodeID(i)
	}

	bit := 0
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			if ((edgeMask >> bit) & 1) == 1 {
				component.edges = append(component.edges, graphEdge{U: i, V: j})
			}
			bit++
		}
	}

	return component
}

func newReport(workload string, worldSize int, nodeName string, networkResult CheckResult, targets map[string]CheckResult) *Report {
	result := networkResult
	if result == CheckResultPass {
		result = CheckResultPass
	}

	return &Report{
		Version:   1,
		Workload:  workload,
		WorldSize: worldSize,
		Result:    result,
		NodeName:  nodeName,
		Checks: Check{
			GPU:       CheckResultPass,
			NIC:       CheckResultPass,
			Storage:   CheckResultPass,
			NodeCheck: CheckResultPass,
			Network: Network{
				Result: networkResult,
				Target: targets,
			},
		},
	}
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
