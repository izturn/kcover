package preflight

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

type batchObservation struct {
	BatchIdx int
	PairA    string
	PairB    string
	Failed   bool
}

type jobKey struct {
	namespace string
	jobName   string
}

type jobState struct {
	expected int
	reports  map[string][]batchObservation
}

// JobCollector 聚合同一个 job 的多份 preflight JSON 报告，报告收齐后输出慢节点。
type JobCollector struct {
	jobs   map[jobKey]*jobState
	config Config
}

func NewJobCollector(cfg Config) *JobCollector {
	return &JobCollector{
		jobs:   make(map[jobKey]*jobState),
		config: cfg.Normalize(),
	}
}

// Add 将单条 JSON 报告并入对应 job。
// 当返回 ready=true 时，badNodes 是完整聚合后的慢节点结论。
func (c *JobCollector) Add(namespace, jobName, reportText string) (ready bool, badNodes []string, err error) {
	if namespace == "" || jobName == "" {
		return false, nil, fmt.Errorf("namespace or job name is empty")
	}

	report, worldSize, observations, err := extractBatchObservations(reportText, c.config.BusBWThresholdGBPS)
	if err != nil {
		return false, nil, fmt.Errorf("extract preflight report: %w", err)
	}

	if worldSize <= 0 {
		return false, nil, fmt.Errorf("invalid world_size: %d", worldSize)
	}

	key := jobKey{namespace: namespace, jobName: jobName}
	state, ok := c.jobs[key]
	if !ok {
		state = &jobState{reports: make(map[string][]batchObservation, worldSize)}
		c.jobs[key] = state
	}

	if worldSize > state.expected {
		state.expected = worldSize
	}
	state.reports[report.NodeName] = observations

	if len(state.reports) < state.expected {
		return false, nil, nil
	}

	badNodes = detectSlowNodes(state.reports, c.config.SlowNodeScore)

	delete(c.jobs, key)
	return true, badNodes, nil
}

func detectSlowNodes(reports map[string][]batchObservation, minScore int) []string {
	type pairKey struct {
		batch int
		a     string
		b     string
	}

	dedup := make(map[pairKey]bool)
	batchIndexSet := make(map[int]struct{})

	for _, obsList := range reports {
		for _, obs := range obsList {
			key := pairKey{batch: obs.BatchIdx, a: obs.PairA, b: obs.PairB}
			if obs.Failed {
				dedup[key] = true
			} else if _, exists := dedup[key]; !exists {
				dedup[key] = false
			}
			batchIndexSet[obs.BatchIdx] = struct{}{}
		}
	}

	if len(batchIndexSet) == 0 {
		return nil
	}

	if minScore <= 0 {
		minScore = DefaultSlowNodeScore
	}

	scores := make(map[string]int)
	for key, failed := range dedup {
		if !failed {
			continue
		}
		scores[key.a]++
		scores[key.b]++
	}

	result := make([]string, 0, len(scores))
	for nodeName, score := range scores {
		if score >= minScore {
			result = append(result, nodeName)
		}
	}
	sort.Strings(result)

	return result
}

func extractBatchObservations(reportText string, threshold float64) (Report, int, []batchObservation, error) {
	report, err := ParseReportText(reportText)
	if err != nil {
		return Report{}, 0, nil, err
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(reportText), &payload); err != nil {
		return Report{}, 0, nil, fmt.Errorf("unmarshal preflight payload: %w", err)
	}

	worldSize, err := intField(payload["world_size"])
	if err != nil {
		worldSize = report.WorldSize
	}
	if worldSize <= 0 {
		return Report{}, 0, nil, fmt.Errorf("invalid world_size")
	}

	batchRaw, _ := payload["batches"].([]any)
	observations := make([]batchObservation, 0, len(batchRaw))
	for _, item := range batchRaw {
		batchMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		batchIdx, err := intField(batchMap["batch_idx"])
		if err != nil {
			continue
		}

		pair, ok := pairField(batchMap["pair"])
		if !ok {
			continue
		}

		var failed bool
		if status, ok := batchMap["status"].(string); ok && strings.EqualFold(status, "fail") {
			failed = true
		} else {
			allreduceMS, errMS := floatField(batchMap["allreduce_ms"])
			allreduceShape, errShape := floatField(batchMap["allreduce_shape"])
			dtypeBytes, errBytes := floatField(batchMap["dtype_bytes"])
			batchWorldSize, errWS := floatField(batchMap["world_size"])
			if errMS != nil || errShape != nil || errBytes != nil || errWS != nil {
				continue
			}

			if allreduceMS <= 0 || allreduceShape <= 0 || dtypeBytes <= 0 || batchWorldSize <= 1 {
				failed = true
			} else {
				payloadBytes := allreduceShape * dtypeBytes
				algoBW := payloadBytes / math.Pow(1024, 3) / (allreduceMS / 1000)
				busBW := algoBW * 2 * (batchWorldSize - 1) / batchWorldSize
				failed = busBW < threshold
			}
		}

		observations = append(observations, batchObservation{
			BatchIdx: batchIdx,
			PairA:    pair[0],
			PairB:    pair[1],
			Failed:   failed,
		})
	}

	return report, worldSize, observations, nil
}

func intField(v any) (int, error) {
	fv, err := floatField(v)
	if err != nil {
		return 0, err
	}
	return int(fv), nil
}

func floatField(v any) (float64, error) {
	switch value := v.(type) {
	case float64:
		return value, nil
	case string:
		if value == "" {
			return 0, fmt.Errorf("empty")
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func pairField(v any) ([2]string, bool) {
	items, ok := v.([]any)
	if !ok || len(items) != 2 {
		return [2]string{}, false
	}

	a, okA := items[0].(string)
	b, okB := items[1].(string)
	if !okA || !okB || a == "" || b == "" {
		return [2]string{}, false
	}

	pair := [2]string{a, b}
	if pair[0] > pair[1] {
		pair[0], pair[1] = pair[1], pair[0]
	}

	return pair, true
}
