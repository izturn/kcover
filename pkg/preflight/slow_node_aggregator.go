package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type batchObservation struct {
	BatchIdx int
	PairA    string
	PairB    string
	SelfID   string
	Failed   bool
}

type jobKey struct {
	namespace string
	jobName   string
}

type jobState struct {
	expectedReports int
	expectedBatches int
	reports         map[string]reportObservations
	createdAt       time.Time
	updatedAt       time.Time
}

type reportObservations struct {
	nodeName     string
	selfID       string
	observations []batchObservation
}

type reportLayout struct {
	reports int
	batches int
}

type ReportCollectionTimeoutError struct {
	Namespace       string
	JobName         string
	ReportedNodes   []string
	ReceivedReports int
	ExpectedReports int
	Timeout         time.Duration
}

// SlowNodeAggregator 聚合同一个 job 的多份 preflight JSON 报告，报告收齐后输出慢节点。
type SlowNodeAggregator struct {
	jobs   map[jobKey]*jobState
	config Settings
	now    func() time.Time
}

var ErrReportCollectionTimeout = errors.New("preflight report collection timed out")

func (e ReportCollectionTimeoutError) Error() string {
	return fmt.Sprintf(
		"%s for %s/%s: got %d/%d reports within %s",
		ErrReportCollectionTimeout,
		e.Namespace,
		e.JobName,
		e.ReceivedReports,
		e.ExpectedReports,
		e.Timeout,
	)
}

func (e ReportCollectionTimeoutError) Unwrap() error {
	return ErrReportCollectionTimeout
}

func (e ReportCollectionTimeoutError) AnchorNodeName() string {
	if len(e.ReportedNodes) == 0 {
		return ""
	}

	return e.ReportedNodes[0]
}

func NewSlowNodeAggregator(cfg Settings) *SlowNodeAggregator {
	return &SlowNodeAggregator{
		jobs:   make(map[jobKey]*jobState),
		config: cfg.Normalize(),
		now:    time.Now,
	}
}

func (c *SlowNodeAggregator) SetNowForTest(now func() time.Time) {
	if now == nil {
		c.now = time.Now
		return
	}

	c.now = now
}

// AddReport 将单条 JSON 报告并入对应 job。
// 当返回 ready=true 时，slowNodes 是完整聚合后的慢节点结论。
func (c *SlowNodeAggregator) AddReport(namespace, jobName, reportText string) (ready bool, slowNodes []string, err error) {
	if namespace == "" || jobName == "" {
		return false, nil, fmt.Errorf("namespace or job name is empty")
	}

	now := c.now()
	if expired, ok := c.expireBefore(now); ok {
		return false, nil, expired
	}

	report, layout, observations, err := extractBatchObservations(reportText, c.config)
	if err != nil {
		return false, nil, fmt.Errorf("extract preflight report: %w", err)
	}

	key := jobKey{namespace: namespace, jobName: jobName}
	state, ok := c.jobs[key]
	if !ok {
		state = &jobState{
			expectedReports: layout.reports,
			expectedBatches: layout.batches,
			reports:         make(map[string]reportObservations, layout.reports),
			createdAt:       now,
			updatedAt:       now,
		}
		c.jobs[key] = state
	}

	if state.expectedReports != layout.reports || state.expectedBatches != layout.batches {
		return false, nil, fmt.Errorf(
			"inconsistent preflight layout for %s/%s: got %d reports/%d batches, want %d reports/%d batches",
			namespace,
			jobName,
			layout.reports,
			layout.batches,
			state.expectedReports,
			state.expectedBatches,
		)
	}
	state.reports[report.NodeName] = reportObservations{nodeName: report.NodeName, selfID: reportIdentity(observations, report.NodeName), observations: observations}
	state.updatedAt = now

	if len(state.reports) < state.expectedReports {
		return false, nil, nil
	}

	slowNodes = detectSlowNodes(state.reports, state.expectedBatches, c.config)

	delete(c.jobs, key)
	return true, slowNodes, nil
}

func (c *SlowNodeAggregator) expireBefore(now time.Time) (ReportCollectionTimeoutError, bool) {
	deadline := c.config.ReportCollectionTimeout
	for key, state := range c.jobs {
		if len(state.reports) >= state.expectedReports {
			continue
		}
		if state.updatedAt.Add(deadline).After(now) {
			continue
		}
		reportedNodes := make([]string, 0, len(state.reports))
		for nodeName := range state.reports {
			reportedNodes = append(reportedNodes, nodeName)
		}
		sort.Strings(reportedNodes)
		delete(c.jobs, key)
		return ReportCollectionTimeoutError{
			Namespace:       key.namespace,
			JobName:         key.jobName,
			ReportedNodes:   reportedNodes,
			ReceivedReports: len(state.reports),
			ExpectedReports: state.expectedReports,
			Timeout:         deadline,
		}, true
	}

	return ReportCollectionTimeoutError{}, false
}

func (c *SlowNodeAggregator) ExpireStale() []ReportCollectionTimeoutError {
	now := c.now()
	var errs []ReportCollectionTimeoutError
	for {
		err, ok := c.expireBefore(now)
		if !ok {
			return errs
		}
		errs = append(errs, err)
	}
}

func detectSlowNodes(reports map[string]reportObservations, expectedBatches int, cfg Settings) []string {
	type pairKey struct {
		batch int
		a     string
		b     string
	}

	dedup := make(map[pairKey]bool)
	batchIndexSet := make(map[int]struct{})

	for _, snapshot := range reports {
		for _, obs := range snapshot.observations {
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

	minScore := cfg.MinimumFailedBatches(expectedBatches)
	identityToNodeName := make(map[string]string, len(reports))
	for _, snapshot := range reports {
		if snapshot.selfID == "" {
			continue
		}
		identityToNodeName[snapshot.selfID] = snapshot.nodeName
	}

	scores := make(map[string]int)
	for key, failed := range dedup {
		if !failed {
			continue
		}
		scores[resolveParticipantName(identityToNodeName, key.a)]++
		scores[resolveParticipantName(identityToNodeName, key.b)]++
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

func extractBatchObservations(reportText string, cfg Settings) (Report, reportLayout, []batchObservation, error) {
	cfg = cfg.Normalize()

	report, err := parseReportText(reportText)
	if err != nil {
		return Report{}, reportLayout{}, nil, err
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(reportText), &payload); err != nil {
		return Report{}, reportLayout{}, nil, fmt.Errorf("unmarshal preflight payload: %w", err)
	}

	batchRaw, _ := payload["batches"].([]any)

	workloadSize, err := intField(payload["workload_size"])
	if err != nil {
		workloadSize, err = intField(payload["world_size"])
	}
	if err != nil {
		workloadSize = report.WorkloadSize
	}
	layout, err := resolveLayout(cfg, workloadSize, len(batchRaw))
	if err != nil {
		return Report{}, reportLayout{}, nil, err
	}

	observationsByBatch := make(map[int]batchObservation, layout.batches)
	for _, item := range batchRaw {
		batchMap, ok := item.(map[string]any)
		if !ok {
			return Report{}, reportLayout{}, nil, fmt.Errorf("invalid batch payload type %T", item)
		}

		batchIdx, err := intField(batchMap["batch_idx"])
		if err != nil {
			return Report{}, reportLayout{}, nil, fmt.Errorf("invalid batch_idx: %w", err)
		}
		if batchIdx < 0 || batchIdx >= layout.batches {
			return Report{}, reportLayout{}, nil, fmt.Errorf("batch_idx %d out of range [0,%d)", batchIdx, layout.batches)
		}
		if _, exists := observationsByBatch[batchIdx]; exists {
			return Report{}, reportLayout{}, nil, fmt.Errorf("duplicate batch_idx %d in report for %s", batchIdx, report.NodeName)
		}

		pair, ok := pairField(batchMap["pair"])
		if !ok {
			return Report{}, reportLayout{}, nil, fmt.Errorf("invalid pair in batch %d", batchIdx)
		}
		selfID, err := batchSelfID(batchMap, report.NodeName, pair)
		if err != nil {
			return Report{}, reportLayout{}, nil, fmt.Errorf("batch %d: %w", batchIdx, err)
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
				return Report{}, reportLayout{}, nil, fmt.Errorf("invalid performance fields in batch %d", batchIdx)
			}

			if allreduceMS <= 0 || allreduceShape <= 0 || dtypeBytes <= 0 || batchWorldSize <= 1 {
				failed = true
			} else {
				payloadBytes := allreduceShape * dtypeBytes
				algoBW := payloadBytes / math.Pow(1024, 3) / (allreduceMS / 1000)
				busBW := algoBW * 2 * (batchWorldSize - 1) / batchWorldSize
				failed = busBW < cfg.BusBWThresholdGBPS
			}
		}

		observationsByBatch[batchIdx] = batchObservation{
			BatchIdx: batchIdx,
			PairA:    pair[0],
			PairB:    pair[1],
			SelfID:   selfID,
			Failed:   failed,
		}
	}

	if len(observationsByBatch) != layout.batches {
		return Report{}, reportLayout{}, nil, fmt.Errorf(
			"incomplete report for %s: got %d batches, want %d",
			report.NodeName,
			len(observationsByBatch),
			layout.batches,
		)
	}

	observations := make([]batchObservation, 0, layout.batches)
	for batchIdx := 0; batchIdx < layout.batches; batchIdx++ {
		observation, exists := observationsByBatch[batchIdx]
		if !exists {
			return Report{}, reportLayout{}, nil, fmt.Errorf("missing batch_idx %d in report for %s", batchIdx, report.NodeName)
		}
		observations = append(observations, observation)
	}

	return report, layout, observations, nil
}

func reportIdentity(observations []batchObservation, fallback string) string {
	for _, observation := range observations {
		if observation.SelfID != "" {
			return observation.SelfID
		}
	}

	return fallback
}

func resolveParticipantName(identityToNodeName map[string]string, participant string) string {
	if nodeName, ok := identityToNodeName[participant]; ok {
		return nodeName
	}

	return participant
}

func resolveLayout(cfg Settings, worldSize int, observedBatches int) (reportLayout, error) {
	layout := reportLayout{}
	if worldSize > 0 {
		layout.reports = worldSize
		layout.batches = worldSize - 1
	} else if cfg.ExpectedReports > 0 && cfg.ExpectedBatches > 0 {
		layout.reports = cfg.ExpectedReports
		layout.batches = cfg.ExpectedBatches
	} else if observedBatches > 0 {
		layout.batches = observedBatches
		layout.reports = observedBatches + 1
	} else {
		return reportLayout{}, fmt.Errorf("cannot resolve preflight layout without world_size, expected layout, or batches")
	}
	if layout.reports <= 1 {
		return reportLayout{}, fmt.Errorf("invalid expected report count: %d", layout.reports)
	}
	if layout.batches <= 0 {
		return reportLayout{}, fmt.Errorf("invalid expected batch count: %d", layout.batches)
	}
	if layout.reports != layout.batches+1 {
		return reportLayout{}, fmt.Errorf("invalid preflight layout: %d reports require %d batches", layout.reports, layout.reports-1)
	}

	return layout, nil
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

func batchSelfID(batchMap map[string]any, nodeName string, pair [2]string) (string, error) {
	if rawSelfIP, ok := batchMap["self_ip"]; ok {
		selfIP, ok := rawSelfIP.(string)
		if !ok || selfIP == "" {
			return "", fmt.Errorf("invalid self_ip")
		}
		if pair[0] != selfIP && pair[1] != selfIP {
			return "", fmt.Errorf("pair %q/%q does not include self_ip %s", pair[0], pair[1], selfIP)
		}
		return selfIP, nil
	}

	if pair[0] != nodeName && pair[1] != nodeName {
		return "", fmt.Errorf("pair %q/%q does not include reporter %s", pair[0], pair[1], nodeName)
	}

	return nodeName, nil
}
