package preflight

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLoadConfigReadsConfigMap(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeyNCCLIBHCA:         "mlx5_0,mlx5_1",
			ConfigKeyBusBWThreshold:    "123.5",
			ConfigKeySlowNodeThreshold: "50%",
			ConfigKeyExpectedReports:   "16",
			ConfigKeyExpectedBatches:   "15",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.NCCLIBHCA != "mlx5_0,mlx5_1" {
		t.Fatalf("cfg.NCCLIBHCA = %q, want %q", cfg.NCCLIBHCA, "mlx5_0,mlx5_1")
	}
	if cfg.BusBWThresholdGBPS != 123.5 {
		t.Fatalf("cfg.BusBWThresholdGBPS = %v, want %v", cfg.BusBWThresholdGBPS, 123.5)
	}
	if cfg.SlowNodeThreshold.Ratio != 0.5 {
		t.Fatalf("cfg.SlowNodeThreshold.Ratio = %v, want %v", cfg.SlowNodeThreshold.Ratio, 0.5)
	}
	if cfg.ExpectedReports != 16 {
		t.Fatalf("cfg.ExpectedReports = %d, want %d", cfg.ExpectedReports, 16)
	}
	if cfg.ExpectedBatches != 15 {
		t.Fatalf("cfg.ExpectedBatches = %d, want %d", cfg.ExpectedBatches, 15)
	}
	if cfg.ReportCollectionTimeout != DefaultReportCollectionTimeout {
		t.Fatalf("cfg.ReportCollectionTimeout = %v, want %v", cfg.ReportCollectionTimeout, DefaultReportCollectionTimeout)
	}
}

func TestLoadConfigDefaultsThresholdWhenMissing(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.BusBWThresholdGBPS != DefaultBusBWThresholdGBPS {
		t.Fatalf("cfg.BusBWThresholdGBPS = %v, want %v", cfg.BusBWThresholdGBPS, DefaultBusBWThresholdGBPS)
	}
	if cfg.SlowNodeThreshold.MinimumBatches != DefaultSlowNodeThresholdBatches {
		t.Fatalf("cfg.SlowNodeThreshold.MinimumBatches = %d, want %d", cfg.SlowNodeThreshold.MinimumBatches, DefaultSlowNodeThresholdBatches)
	}
	if cfg.ExpectedReports != 0 {
		t.Fatalf("cfg.ExpectedReports = %d, want %d", cfg.ExpectedReports, 0)
	}
	if cfg.ExpectedBatches != 0 {
		t.Fatalf("cfg.ExpectedBatches = %d, want %d", cfg.ExpectedBatches, 0)
	}
	if cfg.ReportCollectionTimeout != DefaultReportCollectionTimeout {
		t.Fatalf("cfg.ReportCollectionTimeout = %v, want %v", cfg.ReportCollectionTimeout, DefaultReportCollectionTimeout)
	}
}

func TestLoadConfigDefaultsWhenConfigMapIsMissing(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset()

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.BusBWThresholdGBPS != DefaultBusBWThresholdGBPS {
		t.Fatalf("cfg.BusBWThresholdGBPS = %v, want %v", cfg.BusBWThresholdGBPS, DefaultBusBWThresholdGBPS)
	}
	if cfg.SlowNodeThreshold.MinimumBatches != DefaultSlowNodeThresholdBatches {
		t.Fatalf("cfg.SlowNodeThreshold.MinimumBatches = %d, want %d", cfg.SlowNodeThreshold.MinimumBatches, DefaultSlowNodeThresholdBatches)
	}
	if cfg.ExpectedReports != 0 {
		t.Fatalf("cfg.ExpectedReports = %d, want %d", cfg.ExpectedReports, 0)
	}
	if cfg.ExpectedBatches != 0 {
		t.Fatalf("cfg.ExpectedBatches = %d, want %d", cfg.ExpectedBatches, 0)
	}
	if cfg.ReportCollectionTimeout != DefaultReportCollectionTimeout {
		t.Fatalf("cfg.ReportCollectionTimeout = %v, want %v", cfg.ReportCollectionTimeout, DefaultReportCollectionTimeout)
	}
}

func TestLoadConfigDefaultsForInvalidThreshold(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeyBusBWThreshold: "abc",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want non-nil")
	}
	if cfg.BusBWThresholdGBPS != DefaultBusBWThresholdGBPS {
		t.Fatalf("cfg.BusBWThresholdGBPS = %v, want %v", cfg.BusBWThresholdGBPS, DefaultBusBWThresholdGBPS)
	}
	if cfg.SlowNodeThreshold.MinimumBatches != DefaultSlowNodeThresholdBatches {
		t.Fatalf("cfg.SlowNodeThreshold.MinimumBatches = %d, want %d", cfg.SlowNodeThreshold.MinimumBatches, DefaultSlowNodeThresholdBatches)
	}
}

func TestLoadConfigReturnsErrorForInvalidSlowNodeThreshold(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeySlowNodeThreshold: "abc",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want non-nil")
	}
	if cfg.SlowNodeThreshold.MinimumBatches != DefaultSlowNodeThresholdBatches {
		t.Fatalf("cfg.SlowNodeThreshold.MinimumBatches = %d, want %d", cfg.SlowNodeThreshold.MinimumBatches, DefaultSlowNodeThresholdBatches)
	}
}

func TestLoadConfigReturnsErrorForInvalidExpectedReports(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeyExpectedReports: "abc",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want non-nil")
	}
	if cfg.ExpectedReports != 0 {
		t.Fatalf("cfg.ExpectedReports = %d, want %d", cfg.ExpectedReports, 0)
	}
}

func TestLoadConfigReturnsErrorForInvalidExpectedBatches(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeyExpectedBatches: "abc",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want non-nil")
	}
	if cfg.ExpectedBatches != 0 {
		t.Fatalf("cfg.ExpectedBatches = %d, want %d", cfg.ExpectedBatches, 0)
	}
}

func TestNormalizeDefaultsReportCollectionTimeout(t *testing.T) {
	t.Parallel()

	cfg := (Settings{ReportCollectionTimeout: -1}).Normalize()
	if cfg.ReportCollectionTimeout != DefaultReportCollectionTimeout {
		t.Fatalf("cfg.ReportCollectionTimeout = %v, want %v", cfg.ReportCollectionTimeout, DefaultReportCollectionTimeout)
	}
}

func TestMinimumFailedBatchesUsesThresholdRatio(t *testing.T) {
	t.Parallel()

	cfg := Settings{SlowNodeThreshold: SlowNodeThreshold{Ratio: 0.5}}
	if got := cfg.MinimumFailedBatches(15); got != 8 {
		t.Fatalf("cfg.MinimumFailedBatches(15) = %d, want %d", got, 8)
	}
}

func TestMinimumFailedBatchesUsesAbsoluteThreshold(t *testing.T) {
	t.Parallel()

	cfg := Settings{SlowNodeThreshold: SlowNodeThreshold{MinimumBatches: 3}}
	if got := cfg.MinimumFailedBatches(15); got != 3 {
		t.Fatalf("cfg.MinimumFailedBatches(15) = %d, want %d", got, 3)
	}
}

func TestParseSlowNodeThresholdSupportsIntegerAndPercent(t *testing.T) {
	t.Parallel()

	count, err := parseSlowNodeThreshold("8")
	if err != nil {
		t.Fatalf("parseSlowNodeThreshold(8) error = %v", err)
	}
	if count.MinimumBatches != 8 {
		t.Fatalf("count.MinimumBatches = %d, want %d", count.MinimumBatches, 8)
	}

	ratio, err := parseSlowNodeThreshold("50%")
	if err != nil {
		t.Fatalf("parseSlowNodeThreshold(50%%) error = %v", err)
	}
	if ratio.Ratio != 0.5 {
		t.Fatalf("ratio.Ratio = %v, want %v", ratio.Ratio, 0.5)
	}
}
