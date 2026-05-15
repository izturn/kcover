package preflight

type CheckResult int

const (
	CheckResultSkip CheckResult = iota
	CheckResultPass
	CheckResultFail
)

// Report 表示一个节点上报的一份 preflight 结果。
// NodeName 由 JSON 中的 node_name 提供。
type Report struct {
	Version      int         `json:"version"`
	WorkloadSize int         `json:"workload_size,omitempty"`
	Rank         int         `json:"rank,omitempty"`
	Result       CheckResult `json:"result"`
	Checks       Check       `json:"check"`
	NodeName     string      `json:"node_name,omitempty"`
}

type Check struct {
	GPU       CheckResult `json:"gpu"`
	Storage   CheckResult `json:"storage"`
	NodeCheck CheckResult `json:"node_check"`
}
