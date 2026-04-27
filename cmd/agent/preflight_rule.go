package main

import (
	"fmt"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/podobserver"
	"github.com/baizeai/kcover/pkg/preflight"
	"github.com/baizeai/kcover/pkg/runner"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const preflightReportDir = "/var/lib/kcover/preflight"

type preflightRule struct {
	baseDir string
}

func newPreflightObserver(cli kubernetes.Interface, sink events.Sink) (runner.Runner, error) {
	observer, err := podobserver.New(cli, sink, "preflight pod observer", preflightRule{baseDir: preflightReportDir})
	if err != nil {
		return nil, fmt.Errorf("create preflight pod observer: %w", err)
	}

	return observer, nil
}

func (r preflightRule) OnAdd(*corev1.Pod) []events.Event {
	return nil
}

func (r preflightRule) OnUpdate(oldPod, newPod *corev1.Pod) []events.Event {
	if !shouldHandlePodUpdate(oldPod, newPod) {
		return nil
	}

	report, err := preflight.LoadReportFile(r.baseDir, newPod.Namespace, newPod.Name)
	if err != nil {
		klog.Errorf("load preflight report for pod %s/%s: %v", newPod.Namespace, newPod.Name, err)
		return nil
	}

	return preflight.NodeEvents(newPod.Namespace, newPod.Name, report)
}

func shouldHandlePodUpdate(oldPod, newPod *corev1.Pod) bool {
	if newPod == nil || newPod.Labels[constants.PreflightLabel] != constants.True {
		return false
	}

	return hasNewFailedInitContainer(oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses)
}

func hasNewFailedInitContainer(oldStatuses, newStatuses []corev1.ContainerStatus) bool {
	oldFailed := make(map[string]struct{}, len(oldStatuses))
	for _, status := range oldStatuses {
		if initContainerFailed(status) {
			oldFailed[status.Name] = struct{}{}
		}
	}

	for _, status := range newStatuses {
		if !initContainerFailed(status) {
			continue
		}
		if _, exists := oldFailed[status.Name]; exists {
			continue
		}
		return true
	}

	return false
}

func initContainerFailed(status corev1.ContainerStatus) bool {
	return status.State.Terminated != nil && status.State.Terminated.ExitCode != 0
}
