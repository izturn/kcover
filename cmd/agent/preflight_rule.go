package main

import (
	"fmt"
	"strings"

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

	jobName := newPod.Labels[constants.KubeflowJobLabel]
	candidates := reportNameCandidates(newPod, jobName)
	for _, reportName := range candidates {
		report, reportText, err := preflight.LoadReportPayload(r.baseDir, newPod.Namespace, reportName)
		if err != nil {
			klog.V(4).Infof("load preflight report %s/%s from %s: %v", newPod.Namespace, newPod.Name, reportName, err)
			continue
		}
		if report.NodeName == "" {
			klog.Errorf("preflight report %s/%s from %s has empty node_name", newPod.Namespace, newPod.Name, reportName)
			continue
		}

		return []events.Event{preflight.ReportDeliveryEvent(newPod.Namespace, report.NodeName, jobName, reportText)}
	}

	klog.Errorf("load preflight report for pod %s/%s from candidates %v is failed", newPod.Namespace, newPod.Name, candidates)
	return nil
}

func reportNameCandidates(pod *corev1.Pod, jobName string) []string {
	candidates := []string{pod.Name}

	if rank, ok := petNodeRankFromPodName(pod.Name, jobName); ok {
		candidates = append(candidates, fmt.Sprintf("%s-%s", jobName, rank))
	}

	unique := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		unique = append(unique, candidate)
	}

	return unique
}

func petNodeRankFromPodName(podName, jobName string) (string, bool) {
	if podName == "" || jobName == "" {
		return "", false
	}

	prefix := jobName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return "", false
	}

	suffix := strings.TrimPrefix(podName, prefix)
	index := strings.LastIndex(suffix, "-")
	if index < 0 || index+1 >= len(suffix) {
		return "", false
	}

	rank := suffix[index+1:]
	for _, ch := range rank {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}

	return rank, true
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
