package preflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

type Preflight struct {
	reports []*Report
}

// New 创建一个空的 preflight 聚合器，后续由 Parse 装载原始报告。
func New() *Preflight {
	return &Preflight{
		reports: []*Report{},
	}
}

// Parse 解析一组原始 JSON 报告。每份报告都必须带有 node_name。
func (p *Preflight) Parse(reports []string) error {
	p.reports = p.reports[:0]

	for _, r := range reports {
		report, err := parseReport(r)
		if err != nil {
			return fmt.Errorf("unmarshal preflight report is failed: %w", err)
		}
		p.reports = append(p.reports, report)
	}
	return nil
}

// parseReport 解析单份 preflight JSON 报告。
func parseReport(text string) (*Report, error) {
	type reportWire struct {
		Version   int         `json:"version"`
		Workload  string      `json:"workload,omitempty"`
		WorldSize any         `json:"world_size,omitempty"`
		Rank      any         `json:"rank,omitempty"`
		Result    CheckResult `json:"result"`
		Checks    Check       `json:"check"`
		NodeName  string      `json:"node_name,omitempty"`
	}

	wire := &reportWire{}
	if err := json.Unmarshal([]byte(text), wire); err != nil {
		return nil, fmt.Errorf("unmarshal preflight report is failed: %w", err)
	}

	worldSize, err := parseIntField(wire.WorldSize)
	if err != nil && wire.WorldSize != nil {
		return nil, fmt.Errorf("invalid world_size: %w", err)
	}
	rank, err := parseIntField(wire.Rank)
	if err != nil && wire.Rank != nil {
		return nil, fmt.Errorf("invalid rank: %w", err)
	}

	report := &Report{
		Version:   wire.Version,
		Workload:  wire.Workload,
		WorldSize: worldSize,
		Rank:      rank,
		Result:    wire.Result,
		Checks:    wire.Checks,
		NodeName:  wire.NodeName,
	}

	if report.NodeName == "" {
		return nil, errors.New("report node name is empty")
	}

	return report, nil
}

func parseIntField(value any) (int, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", value)
	}
}

// ParseReportText 解析单份 preflight JSON 报告。
func ParseReportText(text string) (Report, error) {
	report, err := parseReport(text)
	if err != nil {
		return Report{}, err
	}

	return *report, nil
}

// Report 汇总当前已加载的报告，返回整体结论以及可以明确归因的 bad 节点。
//
// 实现方式：
//   - 先把本地检查失败的节点直接归因为 bad 节点。
//   - 再反复应用“过滤已知坏节点后仍然对全部 peer 失败”的收敛规则。
//   - 不再对剩余网络失败做额外图求解，只返回能够直接收敛出的 bad 节点。
func (p *Preflight) Report() (_ CheckResult, badNodes []string, err error) {
	if len(p.reports) == 0 {
		return CheckResultSkip, nil, errors.New("no reports loaded")
	}

	passed := 0
	badNodeSet := make(map[string]struct{})
	netCandidates := make([]*Report, 0, len(p.reports))

	for _, report := range p.reports {
		if report.Result != CheckResultFail { // 跳过也视为 pass
			passed++
			continue
		}
		// 出错，但不是网络问题，且节点信息可用，说明该节点本地检查失败，可以直接归因到节点上
		if report.Checks.Network.Result != CheckResultFail && report.NodeName != "" {
			badNodeSet[report.NodeName] = struct{}{}
			continue
		}
		netCandidates = append(netCandidates, report)
	}

	if passed == len(p.reports) { // 全部通过，直接返回
		return CheckResultPass, nil, nil
	}

	// 先剥离一批可以直接确定的网络异常节点：
	// 每轮都先过滤掉已知坏节点，再检查剩余 target 是否全部失败。
	// 如果某节点在过滤后的视角里仍然对所有 peer 都失败，说明异常已经收敛到该节点侧，
	// 可以直接加入 badNodeSet，并继续影响下一轮过滤。
	for {
		filteredCandidates := filterReports(netCandidates, badNodeSet)
		newBadNodes := nodesWithAllFailedTargets(filteredCandidates)
		if len(newBadNodes) == 0 {
			break
		}

		for _, nodeName := range newBadNodes {
			badNodeSet[nodeName] = struct{}{}
		}
	}

	return CheckResultFail, sortedKeys(badNodeSet), nil
}

// nodesWithAllFailedTargets 找出那些对当前视角里的所有 target 都失败的节点。
//
// 实现方式：
//   - 逐份检查报告里的 network target 结果。
//   - 只有 target 非空且全部为 fail 的节点才会被收集。
//   - 返回值按字典序排序，保证后续收敛过程稳定可测。
func nodesWithAllFailedTargets(reports []*Report) []string {
	nodeSet := make(map[string]struct{})
	for _, report := range reports {
		if report.NodeName == "" || !allTargetsFailed(report.Checks.Network.Target) {
			continue
		}
		nodeSet[report.NodeName] = struct{}{}
	}

	return sortedKeys(nodeSet)
}

// allTargetsFailed 判断一个节点在当前保留的 peer 集合上是否全部网络失败。
func allTargetsFailed(targets map[string]CheckResult) bool {
	if len(targets) == 0 {
		return false
	}

	for _, result := range targets {
		if result != CheckResultFail {
			return false
		}
	}

	return true
}

// filterTargets 删除指向已知坏节点的 target，保留其余方向结果不变。
func filterTargets(targets map[string]CheckResult, excluded map[string]struct{}) map[string]CheckResult {
	if len(excluded) == 0 || len(targets) == 0 {
		return targets
	}

	filtered := make(map[string]CheckResult, len(targets))
	for nodeName, result := range targets {
		if _, exists := excluded[nodeName]; exists {
			continue
		}
		filtered[nodeName] = result
	}

	return filtered
}

// filterReports 删除已知坏节点对应的报告，并同步剔除其余报告里指向这些节点的 target。
// 它还会把 world_size 重写为过滤后的报告数，保持剩余视图自洽。
func filterReports(reports []*Report, excluded map[string]struct{}) []*Report {
	if len(excluded) == 0 {
		return reports
	}

	filteredReports := make([]*Report, 0, len(reports))
	for _, report := range reports {
		if _, exists := excluded[report.NodeName]; exists {
			continue
		}

		filteredReports = append(filteredReports, &Report{
			Version:   report.Version,
			Workload:  report.Workload,
			WorldSize: report.WorldSize,
			Rank:      report.Rank,
			Result:    report.Result,
			NodeName:  report.NodeName,
			Checks: Check{
				GPU:       report.Checks.GPU,
				NIC:       report.Checks.NIC,
				Storage:   report.Checks.Storage,
				NodeCheck: report.Checks.NodeCheck,
				Network: Network{
					Result: report.Checks.Network.Result,
					Target: filterTargets(report.Checks.Network.Target, excluded),
				},
			},
		})
	}

	for _, report := range filteredReports {
		report.WorldSize = len(filteredReports)
	}

	return filteredReports
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)

	return result
}
