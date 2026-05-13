package preflight

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSlowNodeAggregatorDetectsSlowNodeFromScores(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{BusBWThresholdGBPS: 100, SlowNodeThreshold: SlowNodeThreshold{Ratio: 0.75}})

	reports := []string{
		`{"version":1,"workload":"job-a","world_size":"4","rank":"0","node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"1","node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"2","node_name":"node-c","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"3","node_name":"node-d","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	for i := 0; i < len(reports)-1; i++ {
		ready, slowNodes, err := aggregator.AddReport("default", "job-a", reports[i])
		if err != nil {
			t.Fatalf("aggregator.AddReport(...) error = %v", err)
		}
		if ready {
			t.Fatalf("aggregator.AddReport(...) ready = true too early, slowNodes=%v", slowNodes)
		}
	}

	ready, slowNodes, err := aggregator.AddReport("default", "job-a", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if !ready {
		t.Fatal("aggregator.AddReport(...) ready = false, want true")
	}
	if !reflect.DeepEqual(slowNodes, []string{"node-c"}) {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want [node-c]", slowNodes)
	}
}

func TestSlowNodeAggregatorReturnsAllNodesMeetingDefaultThreshold(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{BusBWThresholdGBPS: 100})

	reports := []string{
		`{"version":1,"workload":"job-a","world_size":"4","rank":"0","node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"1","node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"2","node_name":"node-c","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"3","node_name":"node-d","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	for i := 0; i < len(reports)-1; i++ {
		ready, slowNodes, err := aggregator.AddReport("default", "job-a", reports[i])
		if err != nil {
			t.Fatalf("aggregator.AddReport(...) error = %v", err)
		}
		if ready {
			t.Fatalf("aggregator.AddReport(...) ready = true too early, slowNodes=%v", slowNodes)
		}
	}

	ready, slowNodes, err := aggregator.AddReport("default", "job-a", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if !ready {
		t.Fatal("aggregator.AddReport(...) ready = false, want true")
	}
	if !reflect.DeepEqual(slowNodes, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want [node-a node-b node-c node-d]", slowNodes)
	}
}

func TestSlowNodeAggregatorReturnsNoSlowNodeWhenAllPairsPass(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{BusBWThresholdGBPS: 100})

	reports := []string{
		`{"version":1,"workload":"job-b","world_size":2,"rank":0,"node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-b","world_size":2,"rank":1,"node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	_, _, _ = aggregator.AddReport("default", "job-b", reports[0])
	ready, slowNodes, err := aggregator.AddReport("default", "job-b", reports[1])
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if !ready {
		t.Fatal("aggregator.AddReport(...) ready = false, want true")
	}
	if len(slowNodes) != 0 {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want []", slowNodes)
	}
}

func TestSlowNodeAggregatorRequiresCompleteReportMatrix(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(DefaultConfig())
	reports := roundRobinReports(16, map[string]struct{}{"node-03": {}})
	incomplete := strings.Replace(reports[0], `{"batch_idx":14,`, `{`, 1)

	ready, slowNodes, err := aggregator.AddReport("default", "job-c", incomplete)
	if err == nil {
		t.Fatal("aggregator.AddReport(...) error = nil, want non-nil for incomplete report")
	}
	if ready {
		t.Fatalf("aggregator.AddReport(...) ready = %v, want false", ready)
	}
	if slowNodes != nil {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want nil", slowNodes)
	}
}

func TestSlowNodeAggregatorDetectsSlowNodeIn16NodeTopology(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{BusBWThresholdGBPS: 100, SlowNodeThreshold: SlowNodeThreshold{Ratio: 0.5}})
	reports := roundRobinReports(16, map[string]struct{}{"node-03": {}})

	for i := 0; i < len(reports)-1; i++ {
		ready, slowNodes, err := aggregator.AddReport("default", "job-d", reports[i])
		if err != nil {
			t.Fatalf("aggregator.AddReport(...) error = %v", err)
		}
		if ready {
			t.Fatalf("aggregator.AddReport(...) ready = true too early, slowNodes=%v", slowNodes)
		}
	}

	ready, slowNodes, err := aggregator.AddReport("default", "job-d", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if !ready {
		t.Fatal("aggregator.AddReport(...) ready = false, want true")
	}
	if !reflect.DeepEqual(slowNodes, []string{"node-03"}) {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want [node-03]", slowNodes)
	}
}

func TestSlowNodeAggregatorInfersLayoutWithoutWorldSize(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{BusBWThresholdGBPS: 100, SlowNodeThreshold: SlowNodeThreshold{Ratio: 0.5}})
	reports := roundRobinReportsWithoutWorldSize(8, map[string]struct{}{"node-03": {}})

	for i := 0; i < len(reports)-1; i++ {
		ready, slowNodes, err := aggregator.AddReport("default", "job-e", reports[i])
		if err != nil {
			t.Fatalf("aggregator.AddReport(...) error = %v", err)
		}
		if ready {
			t.Fatalf("aggregator.AddReport(...) ready = true too early, slowNodes=%v", slowNodes)
		}
	}

	ready, slowNodes, err := aggregator.AddReport("default", "job-e", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if !ready {
		t.Fatal("aggregator.AddReport(...) ready = false, want true")
	}
	if !reflect.DeepEqual(slowNodes, []string{"node-03"}) {
		t.Fatalf("aggregator.AddReport(...) slowNodes = %v, want [node-03]", slowNodes)
	}
}

func TestExtractBatchObservationsSupportsSuccessfulReportWithSelfIP(t *testing.T) {
	t.Parallel()

	reportText := `{
	  "version": 1,
	  "workload": "pre-4-node-0",
	  "world_size": "4",
	  "rank": "0",
	  "node_name": "c500-worker1",
	  "result": 1,
	  "check": {
	    "storage": 1,
	    "gpu": 1,
	    "node_check": 1
	  },
	  "batches": [
	    {
	      "schema": "v3",
	      "phase": "pairwise",
	      "batch_idx": 0,
	      "pair": ["10.0.0.1", "10.0.0.2"],
	      "self_ip": "10.0.0.1",
	      "local_rank": 0,
	      "allreduce_ms": 1.234,
	      "world_size": 16,
	      "allreduce_shape": 16777216,
	      "dtype_bytes": 4,
	      "ranks_recorded": 8
	    },
	    {
	      "schema": "v3",
	      "phase": "pairwise",
	      "batch_idx": 1,
	      "pair": ["10.0.0.1", "10.0.0.3"],
	      "self_ip": "10.0.0.1",
	      "local_rank": 0,
	      "allreduce_ms": 1.234,
	      "world_size": 16,
	      "allreduce_shape": 16777216,
	      "dtype_bytes": 4,
	      "ranks_recorded": 8
	    },
	    {
	      "schema": "v3",
	      "phase": "pairwise",
	      "batch_idx": 2,
	      "pair": ["10.0.0.1", "10.0.0.4"],
	      "self_ip": "10.0.0.1",
	      "local_rank": 0,
	      "allreduce_ms": 1.234,
	      "world_size": 16,
	      "allreduce_shape": 16777216,
	      "dtype_bytes": 4,
	      "ranks_recorded": 8
	    }
	  ]
	}`

	report, layout, observations, err := extractBatchObservations(reportText, DefaultConfig())
	if err != nil {
		t.Fatalf("extractBatchObservations(...) error = %v", err)
	}
	if report.NodeName != "c500-worker1" {
		t.Fatalf("report.NodeName = %q, want %q", report.NodeName, "c500-worker1")
	}
	if layout.reports != 4 || layout.batches != 3 {
		t.Fatalf("layout = %+v, want reports=4 batches=3", layout)
	}
	if len(observations) != 3 {
		t.Fatalf("len(observations) = %d, want 3", len(observations))
	}
	if observations[0].SelfID != "10.0.0.1" {
		t.Fatalf("observations[0].SelfID = %q, want %q", observations[0].SelfID, "10.0.0.1")
	}
	if observations[0].Failed {
		t.Fatal("observations[0].Failed = true, want false")
	}
}

func TestExtractBatchObservationsSupportsFailedReportWithSelfIP(t *testing.T) {
	t.Parallel()

	reportText := `{
	  "version": 1,
	  "workload": "pre-4-node-0",
	  "world_size": "4",
	  "rank": "0",
	  "node_name": "c500-worker1",
	  "result": 1,
	  "check": {
	    "storage": 1,
	    "gpu": 1,
	    "node_check": 1
	  },
	  "batches": [
	    {
	      "schema": "v3",
	      "batch_idx": 0,
	      "pair": ["10.0.0.1", "10.0.0.2"],
	      "self_ip": "10.0.0.1",
	      "status": "fail",
	      "rc": 1
	    },
	    {
	      "schema": "v3",
	      "batch_idx": 1,
	      "pair": ["10.0.0.1", "10.0.0.3"],
	      "self_ip": "10.0.0.1",
	      "status": "fail",
	      "rc": 1
	    },
	    {
	      "schema": "v3",
	      "batch_idx": 2,
	      "pair": ["10.0.0.1", "10.0.0.4"],
	      "self_ip": "10.0.0.1",
	      "status": "fail",
	      "rc": 1
	    }
	  ]
	}`

	_, layout, observations, err := extractBatchObservations(reportText, DefaultConfig())
	if err != nil {
		t.Fatalf("extractBatchObservations(...) error = %v", err)
	}
	if layout.reports != 4 || layout.batches != 3 {
		t.Fatalf("layout = %+v, want reports=4 batches=3", layout)
	}
	if len(observations) != 3 {
		t.Fatalf("len(observations) = %d, want 3", len(observations))
	}
	if !observations[0].Failed {
		t.Fatal("observations[0].Failed = false, want true")
	}
	if observations[0].PairA != "10.0.0.1" || observations[0].PairB != "10.0.0.2" {
		t.Fatalf("observations[0] pair = %q/%q, want 10.0.0.1/10.0.0.2", observations[0].PairA, observations[0].PairB)
	}
}

func TestSlowNodeAggregatorExpiresIncompleteJob(t *testing.T) {
	t.Parallel()

	aggregator := NewSlowNodeAggregator(Settings{
		ReportCollectionTimeout: 10 * time.Second,
	})
	now := time.Unix(100, 0)
	aggregator.now = func() time.Time { return now }

	report := `{"version":1,"workload":"job-a","world_size":2,"rank":0,"node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"batch_idx":0,"pair":["node-a","node-b"],"status":"fail"}]}`
	ready, slowNodes, err := aggregator.AddReport("default", "job-a", report)
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) error = %v", err)
	}
	if ready || slowNodes != nil {
		t.Fatalf("first AddReport = (%v, %v), want (false, nil)", ready, slowNodes)
	}

	now = now.Add(11 * time.Second)
	errs := aggregator.ExpireStale()
	if len(errs) != 1 {
		t.Fatalf("len(ExpireStale()) = %d, want 1", len(errs))
	}
	if errs[0].AnchorNodeName() != "node-a" {
		t.Fatalf("errs[0].AnchorNodeName() = %q, want node-a", errs[0].AnchorNodeName())
	}
	if !strings.Contains(errs[0].Error(), "got 1/2 reports") {
		t.Fatalf("ExpireStale error = %q, want report count detail", errs[0])
	}
	if len(aggregator.jobs) != 0 {
		t.Fatalf("len(aggregator.jobs) = %d, want 0", len(aggregator.jobs))
	}
	if !errors.Is(errs[0], ErrReportCollectionTimeout) {
		t.Fatalf("ExpireStale error = %v, want ErrReportCollectionTimeout", errs[0])
	}

	ready, slowNodes, err = aggregator.AddReport("default", "job-a", report)
	if err != nil {
		t.Fatalf("aggregator.AddReport(...) after expiry error = %v", err)
	}
	if ready || slowNodes != nil {
		t.Fatalf("AddReport after expiry = (%v, %v), want (false, nil)", ready, slowNodes)
	}
}

func roundRobinReports(nodeCount int, failingNodes map[string]struct{}) []string {
	nodeNames := make([]string, 0, nodeCount)
	for idx := 0; idx < nodeCount; idx++ {
		nodeNames = append(nodeNames, fmt.Sprintf("node-%02d", idx))
	}

	schedule := roundRobinPairs(nodeNames)
	reports := make([]string, 0, nodeCount)
	for rank, nodeName := range nodeNames {
		batches := make([]string, 0, len(schedule))
		for batchIdx, pairs := range schedule {
			for _, pair := range pairs {
				if pair[0] != nodeName && pair[1] != nodeName {
					continue
				}

				entry := fmt.Sprintf(`{"batch_idx":%d,"pair":["%s","%s"]`, batchIdx, pair[0], pair[1])
				if _, exists := failingNodes[nodeName]; exists {
					entry += `,"status":"fail"}`
				} else {
					entry += `,"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}`
				}
				batches = append(batches, entry)
				break
			}
		}

		reports = append(reports, fmt.Sprintf(
			`{"version":1,"workload":"job-16","world_size":%d,"rank":%d,"node_name":"%s","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[%s]}`,
			nodeCount,
			rank,
			nodeName,
			strings.Join(batches, ","),
		))
	}

	return reports
}

func roundRobinReportsWithoutWorldSize(nodeCount int, failingNodes map[string]struct{}) []string {
	reports := roundRobinReports(nodeCount, failingNodes)
	trimmed := make([]string, 0, len(reports))
	for _, report := range reports {
		trimmed = append(trimmed, strings.Replace(report, fmt.Sprintf(`"world_size":%d,`, nodeCount), "", 1))
	}

	return trimmed
}

func roundRobinPairs(nodeNames []string) [][][2]string {
	working := append([]string(nil), nodeNames...)
	batchCount := len(working) - 1
	batches := make([][][2]string, 0, batchCount)

	for batchIdx := 0; batchIdx < batchCount; batchIdx++ {
		pairs := make([][2]string, 0, len(working)/2)
		for left := 0; left < len(working)/2; left++ {
			right := len(working) - 1 - left
			pair := [2]string{working[left], working[right]}
			if pair[0] > pair[1] {
				pair[0], pair[1] = pair[1], pair[0]
			}
			pairs = append(pairs, pair)
		}
		batches = append(batches, pairs)

		rotated := append([]string{working[0], working[len(working)-1]}, working[1:len(working)-1]...)
		working = rotated
	}

	return batches
}
