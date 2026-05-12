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
			ConfigKeyNCCLIBHCA:      "mlx5_0,mlx5_1",
			ConfigKeyBusBWThreshold: "123.5",
			ConfigKeySlowNodeScore:  "3",
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
	if cfg.SlowNodeScore != 3 {
		t.Fatalf("cfg.SlowNodeScore = %d, want %d", cfg.SlowNodeScore, 3)
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
	if cfg.SlowNodeScore != DefaultSlowNodeScore {
		t.Fatalf("cfg.SlowNodeScore = %d, want %d", cfg.SlowNodeScore, DefaultSlowNodeScore)
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
	if cfg.SlowNodeScore != DefaultSlowNodeScore {
		t.Fatalf("cfg.SlowNodeScore = %d, want %d", cfg.SlowNodeScore, DefaultSlowNodeScore)
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
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.BusBWThresholdGBPS != DefaultBusBWThresholdGBPS {
		t.Fatalf("cfg.BusBWThresholdGBPS = %v, want %v", cfg.BusBWThresholdGBPS, DefaultBusBWThresholdGBPS)
	}
	if cfg.SlowNodeScore != DefaultSlowNodeScore {
		t.Fatalf("cfg.SlowNodeScore = %d, want %d", cfg.SlowNodeScore, DefaultSlowNodeScore)
	}
}

func TestLoadConfigDefaultsForInvalidSlowNodeScore(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "kcover-system"},
		Data: map[string]string{
			ConfigKeySlowNodeScore: "abc",
		},
	})

	cfg, err := LoadConfig(context.Background(), cli, "kcover-system")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.SlowNodeScore != DefaultSlowNodeScore {
		t.Fatalf("cfg.SlowNodeScore = %d, want %d", cfg.SlowNodeScore, DefaultSlowNodeScore)
	}
}
