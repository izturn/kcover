package preflight

type CheckResult int

const (
	CheckResultSkip CheckResult = iota
	CheckResultPass
	CheckResultFail
)

// Report 表示一个节点上报的一份 preflight 结果。
type Report struct {
	Version      int    `json:"version"`
	Workload     string `json:"workload,omitempty"`
	WorkloadSize int    `json:"workload_size,omitempty"`
	Rank         int    `json:"rank,omitempty"`
	NodeName     string `json:"node_name,omitempty"`
	NodeIP       string `json:"node_ip,omitempty"`

	// checks
	GPUCheck     CheckResult `json:"gpu_check,omitempty"`
	StorageCheck CheckResult `json:"storage_check,omitempty"`

	// node_check 单独处理
}
