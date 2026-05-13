package preflight

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	DefaultBusBWThresholdGBPS       = 5.0
	DefaultSlowNodeThresholdBatches = 1
	DefaultReportCollectionTimeout  = 30 * time.Minute
	ConfigMapName                   = "preflight-config"
	ConfigKeyNCCLIBHCA              = "NCCL_IB_HCA"
	ConfigKeyBusBWThreshold         = "BUSBW_THRESHOLD_GBPS"
	ConfigKeySlowNodeThreshold      = "SLOW_NODE_THRESHOLD"
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
		SlowNodeThreshold:       SlowNodeThreshold{MinimumBatches: DefaultSlowNodeThresholdBatches},
		ReportCollectionTimeout: DefaultReportCollectionTimeout,
	}
}

func (cfg Settings) Normalize() Settings {
	if cfg.BusBWThresholdGBPS <= 0 {
		cfg.BusBWThresholdGBPS = DefaultBusBWThresholdGBPS
	}
	if cfg.SlowNodeThreshold.MinimumBatches <= 0 && cfg.SlowNodeThreshold.Ratio <= 0 {
		cfg.SlowNodeThreshold.MinimumBatches = DefaultSlowNodeThresholdBatches
	}
	if cfg.SlowNodeThreshold.MinimumBatches < 0 {
		cfg.SlowNodeThreshold.MinimumBatches = 0
	}
	if cfg.SlowNodeThreshold.Ratio < 0 {
		cfg.SlowNodeThreshold.Ratio = 0
	}
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
	cfg = cfg.Normalize()

	if cfg.SlowNodeThreshold.MinimumBatches > 0 {
		return cfg.SlowNodeThreshold.MinimumBatches
	}

	if expectedBatches <= 0 {
		return 1
	}

	score := int(math.Ceil(float64(expectedBatches) * cfg.SlowNodeThreshold.Ratio))
	if score < 1 {
		return 1
	}
	return score
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
	if raw := strings.TrimSpace(cm.Data[ConfigKeySlowNodeThreshold]); raw != "" {
		threshold, err := parseSlowNodeThreshold(raw)
		if err != nil {
			return DefaultConfig(), fmt.Errorf("parse %s: %w", ConfigKeySlowNodeThreshold, err)
		}
		cfg.SlowNodeThreshold = threshold
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

func parseSlowNodeThreshold(raw string) (SlowNodeThreshold, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return SlowNodeThreshold{}, fmt.Errorf("empty threshold")
	}

	if strings.HasSuffix(value, "%") {
		ratioText := strings.TrimSpace(strings.TrimSuffix(value, "%"))
		ratio, err := strconv.ParseFloat(ratioText, 64)
		if err != nil {
			return SlowNodeThreshold{}, err
		}
		if ratio <= 0 || ratio > 100 {
			return SlowNodeThreshold{}, fmt.Errorf("percentage %v out of range (0,100]", ratio)
		}
		return SlowNodeThreshold{Ratio: ratio / 100}, nil
	}

	if count, err := strconv.Atoi(value); err == nil {
		if count <= 0 {
			return SlowNodeThreshold{}, fmt.Errorf("batch count %d must be positive", count)
		}
		return SlowNodeThreshold{MinimumBatches: count}, nil
	}

	ratio, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return SlowNodeThreshold{}, err
	}
	if ratio <= 0 || ratio > 1 {
		return SlowNodeThreshold{}, fmt.Errorf("ratio %v out of range (0,1]", ratio)
	}

	return SlowNodeThreshold{Ratio: ratio}, nil
}
