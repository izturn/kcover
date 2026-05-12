package preflight

import (
	"reflect"
	"testing"
)

func TestJobCollectorDetectsSlowNodeFromScores(t *testing.T) {
	t.Parallel()

	collector := NewJobCollector(Config{BusBWThresholdGBPS: 100, SlowNodeScore: 3})

	reports := []string{
		`{"version":1,"workload":"job-a","world_size":"4","rank":"0","node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"1","node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"2","node_name":"node-c","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"3","node_name":"node-d","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	for i := 0; i < len(reports)-1; i++ {
		ready, badNodes, err := collector.Add("default", "job-a", reports[i])
		if err != nil {
			t.Fatalf("collector.Add(...) error = %v", err)
		}
		if ready {
			t.Fatalf("collector.Add(...) ready = true too early, badNodes=%v", badNodes)
		}
	}

	ready, badNodes, err := collector.Add("default", "job-a", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("collector.Add(...) error = %v", err)
	}
	if !ready {
		t.Fatal("collector.Add(...) ready = false, want true")
	}
	if !reflect.DeepEqual(badNodes, []string{"node-c"}) {
		t.Fatalf("collector.Add(...) badNodes = %v, want [node-c]", badNodes)
	}
}

func TestJobCollectorReturnsAllNodesMeetingDefaultScore(t *testing.T) {
	t.Parallel()

	collector := NewJobCollector(Config{BusBWThresholdGBPS: 100})

	reports := []string{
		`{"version":1,"workload":"job-a","world_size":"4","rank":"0","node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"1","node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"2","node_name":"node-c","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-a","node-c"],"status":"fail","rc":1},{"schema":"v3","batch_idx":2,"pair":["node-b","node-c"],"status":"fail","rc":1}]}`,
		`{"version":1,"workload":"job-a","world_size":"4","rank":"3","node_name":"node-d","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-c","node-d"],"status":"fail","rc":1},{"schema":"v3","batch_idx":1,"pair":["node-b","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4},{"schema":"v3","batch_idx":2,"pair":["node-a","node-d"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	for i := 0; i < len(reports)-1; i++ {
		ready, badNodes, err := collector.Add("default", "job-a", reports[i])
		if err != nil {
			t.Fatalf("collector.Add(...) error = %v", err)
		}
		if ready {
			t.Fatalf("collector.Add(...) ready = true too early, badNodes=%v", badNodes)
		}
	}

	ready, badNodes, err := collector.Add("default", "job-a", reports[len(reports)-1])
	if err != nil {
		t.Fatalf("collector.Add(...) error = %v", err)
	}
	if !ready {
		t.Fatal("collector.Add(...) ready = false, want true")
	}
	if !reflect.DeepEqual(badNodes, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("collector.Add(...) badNodes = %v, want [node-a node-b node-c node-d]", badNodes)
	}
}

func TestJobCollectorReturnsNoSlowNodeWhenAllPairsPass(t *testing.T) {
	t.Parallel()

	collector := NewJobCollector(Config{BusBWThresholdGBPS: 100})

	reports := []string{
		`{"version":1,"workload":"job-b","world_size":2,"rank":0,"node_name":"node-a","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
		`{"version":1,"workload":"job-b","world_size":2,"rank":1,"node_name":"node-b","result":2,"check":{"storage":2,"gpu":2,"node_check":2},"batches":[{"schema":"v3","batch_idx":0,"pair":["node-a","node-b"],"allreduce_ms":0.5,"world_size":16,"allreduce_shape":16777216,"dtype_bytes":4}]}`,
	}

	_, _, _ = collector.Add("default", "job-b", reports[0])
	ready, badNodes, err := collector.Add("default", "job-b", reports[1])
	if err != nil {
		t.Fatalf("collector.Add(...) error = %v", err)
	}
	if !ready {
		t.Fatal("collector.Add(...) ready = false, want true")
	}
	if len(badNodes) != 0 {
		t.Fatalf("collector.Add(...) badNodes = %v, want []", badNodes)
	}
}
