package preflight

type CheckResult int

const (
	CheckResultSkip CheckResult = iota
	CheckResultPass
	CheckResultFail
)

// directionalAnomaly 记录单向网络失败，这类失败会被刻意排除在节点归因之外。
//
// 实现方式：
//   - diagnoseNetwork 会对每一对节点做双向比较。
//   - 如果只失败了一个方向，这对节点就会被转成一个 directionalAnomaly。
//   - 该异常会原样返回给调用方，而不会被并入用于节点归因的双向失败图。
type directionalAnomaly struct {
	From string
	To   string
}

// networkDiagnosis 汇总同一 workload 下所有 peer 报告的网络归因结果。
//
// 实现方式：
//   - badNodes 保存所有最小代价解释里都必须出现的节点。
//   - suspectNodes 保存至少出现在一个最小代价解释里、但不是所有解释都需要的节点。
//   - directionalIssues 保存节点两两比较时发现的单向失败，这部分被刻意排除在节点归因之外。
type networkDiagnosis struct {
	badNodes          []string
	suspectNodes      []string
	directionalIssues []directionalAnomaly
}

type graphEdge struct {
	U int
	V int
}

type graphComponent struct {
	nodes []string
	edges []graphEdge
}

// Report 表示一个节点上报的一份 preflight 结果。
// NodeName 由 JSON 中的 node_name 提供。
type Report struct {
	Version   int         `json:"version"`
	Workload  string      `json:"workload,omitempty"`
	WorldSize int         `json:"world_size,omitempty"`
	Rank      int         `json:"rank,omitempty"`
	Result    CheckResult `json:"result"`
	Checks    Check       `json:"check"`
	NodeName  string      `json:"node_name,omitempty"`
}

type Check struct {
	GPU       CheckResult `json:"gpu"`
	NIC       CheckResult `json:"nic"`
	Network   Network     `json:"network"`
	Storage   CheckResult `json:"storage"`
	NodeCheck CheckResult `json:"node_check"`
}

type Network struct {
	Result CheckResult            `json:"result"`
	Target map[string]CheckResult `json:"target"`
}
