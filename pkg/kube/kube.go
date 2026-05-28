package kube

import (
	"context"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/baizeai/kcover/pkg/constants"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const UnschedulableNodeTaintKey = "node.kubernetes.io/unschedulable"
const requestTimeout = 10 * time.Second

var serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

func WithRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, requestTimeout)
}

func NodeNameFromEnv() string {
	if nodeName := os.Getenv(constants.NodeNameEnv); nodeName != "" {
		return nodeName
	}

	return os.Getenv(constants.LegacyNodeNameEnv)
}

func CurrentNamespace() string {
	if data, err := os.ReadFile(serviceAccountNamespacePath); err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace
		}
	}

	return "default"
}

func TaintNodeUnschedulable(ctx context.Context, cli kubernetes.Interface, nodeName string) error {
	taint := corev1.Taint{
		Key:    UnschedulableNodeTaintKey,
		Effect: corev1.TaintEffectNoSchedule,
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := cli.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		changed := !node.Spec.Unschedulable
		node.Spec.Unschedulable = true

		if slices.IndexFunc(node.Spec.Taints, func(existing corev1.Taint) bool {
			return existing.Key == taint.Key && existing.Effect == taint.Effect
		}) == -1 {
			node.Spec.Taints = append(node.Spec.Taints, taint)
			changed = true
		}

		if !changed {
			return nil
		}

		_, err = cli.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		return err
	})
}

func GetK8sConfigConfigWithFile(kubeconfig, context string) *rest.Config {
	var config *rest.Config
	if kubeconfig == "" && context == "" {
		config, _ := rest.InClusterConfig()
		if config != nil {
			return config
		}
	}
	if kubeconfig != "" {
		info, err := os.Stat(kubeconfig)
		if err != nil || info.Size() == 0 {
			// If the specified kubeconfig doesn't exists / empty file / any other error
			// from file stat, fall back to default
			kubeconfig = ""
		}
	}

	// Config loading rules:
	// 1. kubeconfig if it not empty string
	// 2. In cluster config if running in-cluster
	// 3. Config(s) in KUBECONFIG environment variable
	// 4. Use $HOME/.kube/config
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.DefaultClientConfig = &clientcmd.DefaultClientConfig
	loadingRules.ExplicitPath = kubeconfig
	configOverrides := &clientcmd.ConfigOverrides{
		ClusterDefaults: clientcmd.ClusterDefaults,
		CurrentContext:  context,
	}

	config, _ = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
	return config
}
