package kube

import (
	"context"
	"os"
	"testing"

	"github.com/baizeai/kcover/pkg/constants"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNodeNameFromEnvPrefersNodeName(t *testing.T) {
	t.Setenv(constants.NodeNameEnv, "node-new")
	t.Setenv(constants.LegacyNodeNameEnv, "node-old")

	if got := NodeNameFromEnv(); got != "node-new" {
		t.Fatalf("NodeNameFromEnv() = %q, want %q", got, "node-new")
	}
}

func TestNodeNameFromEnvFallsBackToLegacyName(t *testing.T) {
	t.Setenv(constants.NodeNameEnv, "")
	t.Setenv(constants.LegacyNodeNameEnv, "node-old")

	if got := NodeNameFromEnv(); got != "node-old" {
		t.Fatalf("NodeNameFromEnv() = %q, want %q", got, "node-old")
	}
}

func TestCurrentNamespaceFallsBackToDefault(t *testing.T) {
	t.Parallel()

	originalPath := serviceAccountNamespacePath
	serviceAccountNamespacePath = t.TempDir() + "/missing"
	t.Cleanup(func() {
		serviceAccountNamespacePath = originalPath
	})

	if got := CurrentNamespace(); got != "default" {
		t.Fatalf("CurrentNamespace() = %q, want %q", got, "default")
	}
}

func TestCurrentNamespaceReadsServiceAccountFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := dir + "/namespace"
	if err := os.WriteFile(path, []byte("baize-system"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	originalPath := serviceAccountNamespacePath
	serviceAccountNamespacePath = path
	t.Cleanup(func() {
		serviceAccountNamespacePath = originalPath
	})

	if got := CurrentNamespace(); got != "baize-system" {
		t.Fatalf("CurrentNamespace() = %q, want %q", got, "baize-system")
	}
}

func TestTaintNodeUnschedulable(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})

	if err := TaintNodeUnschedulable(context.Background(), cli, "node-a"); err != nil {
		t.Fatalf("TaintNodeUnschedulable() error = %v", err)
	}

	node, err := cli.CoreV1().Nodes().Get(context.Background(), "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(node-a) error = %v", err)
	}
	if !node.Spec.Unschedulable {
		t.Fatal("node.Spec.Unschedulable = false, want true")
	}
	if len(node.Spec.Taints) != 1 {
		t.Fatalf("len(node.Spec.Taints) = %d, want 1", len(node.Spec.Taints))
	}
	if node.Spec.Taints[0].Key != UnschedulableNodeTaintKey {
		t.Fatalf("node.Spec.Taints[0].Key = %q, want %q", node.Spec.Taints[0].Key, UnschedulableNodeTaintKey)
	}
	if node.Spec.Taints[0].Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("node.Spec.Taints[0].Effect = %q, want %q", node.Spec.Taints[0].Effect, corev1.TaintEffectNoSchedule)
	}
}

func TestTaintNodeUnschedulableDoesNotDuplicateTaint(t *testing.T) {
	t.Parallel()

	cli := fake.NewSimpleClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec: corev1.NodeSpec{
			Unschedulable: true,
			Taints: []corev1.Taint{{
				Key:    UnschedulableNodeTaintKey,
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
	})

	if err := TaintNodeUnschedulable(context.Background(), cli, "node-a"); err != nil {
		t.Fatalf("TaintNodeUnschedulable() error = %v", err)
	}

	node, err := cli.CoreV1().Nodes().Get(context.Background(), "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(node-a) error = %v", err)
	}
	if len(node.Spec.Taints) != 1 {
		t.Fatalf("len(node.Spec.Taints) = %d, want 1", len(node.Spec.Taints))
	}
}
