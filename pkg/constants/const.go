package constants

const (
	KubeflowJobLabel  = "training.kubeflow.org/job-name"
	PreflightLabel    = "kcover.io/preflight"
	NodeNameEnv       = "NODE_NAME"
	LegacyNodeNameEnv = "FAST_RECOVERY_NODE_NAME"
	// recovery annotations
	NeedRecoveryAnnotation = "kcover.io/need-recovery"

	EnabledRecoveryLabel = "kcover.io/cascading-recovery"

	True = "true"
)
