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
