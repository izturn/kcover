package preflight

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	DefaultBusBWThresholdGBPS = 5.0
	DefaultSlowNodeScore      = 1
	ConfigMapName             = "preflight-config"
	ConfigKeyNCCLIBHCA        = "NCCL_IB_HCA"
	ConfigKeyBusBWThreshold   = "BUSBW_THRESHOLD_GBPS"
	ConfigKeySlowNodeScore    = "SLOW_NODE_SCORE"
)

type Config struct {
	NCCLIBHCA          string
	BusBWThresholdGBPS float64
	SlowNodeScore      int
}

func DefaultConfig() Config {
	return Config{BusBWThresholdGBPS: DefaultBusBWThresholdGBPS, SlowNodeScore: DefaultSlowNodeScore}
}

func (cfg Config) Normalize() Config {
	if cfg.BusBWThresholdGBPS <= 0 {
		cfg.BusBWThresholdGBPS = DefaultBusBWThresholdGBPS
	}
	if cfg.SlowNodeScore <= 0 {
		cfg.SlowNodeScore = DefaultSlowNodeScore
	}

	return cfg
}

func LoadConfig(ctx context.Context, cli kubernetes.Interface, namespace string) (Config, error) {
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

	cfg := Config{
		NCCLIBHCA: strings.TrimSpace(cm.Data[ConfigKeyNCCLIBHCA]),
	}
	if raw := strings.TrimSpace(cm.Data[ConfigKeyBusBWThreshold]); raw != "" {
		threshold, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return DefaultConfig(), nil
		}
		cfg.BusBWThresholdGBPS = threshold
	}
	if raw := strings.TrimSpace(cm.Data[ConfigKeySlowNodeScore]); raw != "" {
		score, err := strconv.Atoi(raw)
		if err != nil {
			return DefaultConfig(), nil
		}
		cfg.SlowNodeScore = score
	}

	return cfg.Normalize(), nil
}
