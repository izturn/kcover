package preflight

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	DefaultBusBWThresholdGBPS       = 5.0
	DefaultReportCollectionTimeout  = 30 * time.Minute
	ConfigMapName                   = "preflight-config"
	ConfigKeyNCCLIBHCA              = "NCCL_IB_HCA"
	ConfigKeyBusBWThreshold         = "BUSBW_THRESHOLD_GBPS"
	ConfigKeyExpectedReports        = "EXPECTED_REPORTS"
	ConfigKeyExpectedBatches        = "EXPECTED_BATCHES_PER_REPORT"
)

type SlowNodeThreshold struct {
	MinimumBatches int
	Ratio          float64
}

type Settings struct {
	NCCLIBHCA               string
	BusBWThresholdGBPS      float64
	SlowNodeThreshold       SlowNodeThreshold
	ExpectedReports         int
	ExpectedBatches         int
	ReportCollectionTimeout time.Duration
}

func DefaultConfig() Settings {
	return Settings{
		BusBWThresholdGBPS:      DefaultBusBWThresholdGBPS,
		SlowNodeThreshold:       SlowNodeThreshold{Ratio: 1},
		ReportCollectionTimeout: DefaultReportCollectionTimeout,
	}
}

func (cfg Settings) Normalize() Settings {
	if cfg.BusBWThresholdGBPS <= 0 {
		cfg.BusBWThresholdGBPS = DefaultBusBWThresholdGBPS
	}
	cfg.SlowNodeThreshold = SlowNodeThreshold{Ratio: 1}
	if cfg.ExpectedReports < 0 {
		cfg.ExpectedReports = 0
	}
	if cfg.ExpectedBatches < 0 {
		cfg.ExpectedBatches = 0
	}
	if cfg.ReportCollectionTimeout <= 0 {
		cfg.ReportCollectionTimeout = DefaultReportCollectionTimeout
	}

	return cfg
}

func (cfg Settings) MinimumFailedBatches(expectedBatches int) int {
	if expectedBatches <= 0 {
		return 1
	}

	return expectedBatches
}

func LoadConfig(ctx context.Context, cli kubernetes.Interface, namespace string) (Settings, error) {
	if cli == nil {
		return DefaultConfig(), fmt.Errorf("kubernetes client is nil")
	}
	if namespace == "" {
		return DefaultConfig(), fmt.Errorf("namespace is empty")
	}

	cm, err := cli.CoreV1().ConfigMaps(namespace).Get(ctx, ConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return DefaultConfig(), nil
		}
		return DefaultConfig(), fmt.Errorf("get configmap %s/%s: %w", namespace, ConfigMapName, err)
	}

	cfg := Settings{
		NCCLIBHCA: strings.TrimSpace(cm.Data[ConfigKeyNCCLIBHCA]),
	}
	if raw := strings.TrimSpace(cm.Data[ConfigKeyBusBWThreshold]); raw != "" {
		threshold, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return DefaultConfig(), fmt.Errorf("parse %s: %w", ConfigKeyBusBWThreshold, err)
		}
		cfg.BusBWThresholdGBPS = threshold
	}
	if raw := strings.TrimSpace(cm.Data[ConfigKeyExpectedReports]); raw != "" {
		reports, err := strconv.Atoi(raw)
		if err != nil {
			return DefaultConfig(), fmt.Errorf("parse %s: %w", ConfigKeyExpectedReports, err)
		}
		cfg.ExpectedReports = reports
	}
	if raw := strings.TrimSpace(cm.Data[ConfigKeyExpectedBatches]); raw != "" {
		batches, err := strconv.Atoi(raw)
		if err != nil {
			return DefaultConfig(), fmt.Errorf("parse %s: %w", ConfigKeyExpectedBatches, err)
		}
		cfg.ExpectedBatches = batches
	}
	return cfg.Normalize(), nil
}
