package main

import (
	"fmt"
	"strconv"
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

// /var/lib/kcover/preflight/<namespace>/<JOB_NAME>-<PET_NODE_RANK>.json
const preflightReportDir = "/var/lib/kcover/preflight"
const preflightInitContainerName = "preflight"

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

	workloadName := preflightWorkloadName(newPod)
	reportName, ok := preflightReportName(newPod, workloadName)
	if !ok {
		klog.V(2).Infof("skip preflight report for pod %s/%s: can not derive report name from workload=%q", newPod.Namespace, newPod.Name, workloadName)
		return nil
	}

	reportText, nodeName, err := preflight.LoadReportPayload(r.baseDir, newPod.Namespace, reportName)
	if err != nil {
		klog.V(2).Infof("load preflight report %s/%s from %s: %v", newPod.Namespace, newPod.Name, reportName, err)
		return nil
	}
	if nodeName == "" {
		klog.Errorf("preflight report %s/%s from %s has empty node_name", newPod.Namespace, newPod.Name, reportName)
		return nil
	}

	event, err := preflight.ReportToEvent(newPod.Namespace, nodeName, workloadName, reportText)
	if err != nil {
		klog.Errorf("build preflight delivery event for %s/%s from %s: %v", newPod.Namespace, newPod.Name, reportName, err)
		return nil
	}

	return []events.Event{event}
}

func preflightWorkloadName(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}

	labels := pod.Labels
	if labels == nil {
		return ""
	}
	if name := labels[constants.LeaderWorkerSetNameLabel]; name != "" {
		return name
	}

	return labels[constants.BatchJobNameLabel]
}

func preflightReportName(pod *corev1.Pod, workloadName string) (string, bool) {
	if pod == nil || pod.Name == "" || workloadName == "" {
		return "", false
	}

	rank, ok := preflightRank(pod, workloadName)
	if !ok {
		return "", false
	}

	return fmt.Sprintf("%s-%s", workloadName, rank), true
}

func preflightRank(pod *corev1.Pod, workloadName string) (string, bool) {
	if pod == nil || workloadName == "" {
		return "", false
	}

	labels := pod.Labels
	if labels[constants.LeaderWorkerSetNameLabel] != "" {
		// TODO: derive LWS rank from authoritative workload metadata when available.
		return petNodeRankFromPodName(pod.Name, workloadName)
	}

	annotations := pod.Annotations
	raw := strings.TrimSpace(annotations[constants.BatchJobCompletionIndexAnnotation])
	if raw == "" {
		return "", false
	}
	if _, err := strconv.Atoi(raw); err != nil {
		return "", false
	}

	return raw, true
}

func petNodeRankFromPodName(podName, workloadName string) (string, bool) {
	if podName == "" || workloadName == "" {
		return "", false
	}

	prefix := workloadName + "-"
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

	return isPreflightFailed(oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses)
}

// isPreflightFailed returns true only when the init container named "preflight"
// transitions from non-failed (or missing) to failed on this update.
func isPreflightFailed(oldStatuses, newStatuses []corev1.ContainerStatus) bool {
	newStatus, ok := initContainerStatusByName(newStatuses, preflightInitContainerName)
	if !ok || !initContainerFailed(newStatus) {
		return false
	}

	oldStatus, ok := initContainerStatusByName(oldStatuses, preflightInitContainerName)
	if !ok {
		return true
	}

	return !initContainerFailed(oldStatus)
}

func initContainerFailed(status corev1.ContainerStatus) bool {
	return status.State.Terminated != nil && status.State.Terminated.ExitCode != 0
}

func initContainerStatusByName(statuses []corev1.ContainerStatus, name string) (corev1.ContainerStatus, bool) {
	for _, status := range statuses {
		if status.Name == name {
			return status, true
		}
	}

	return corev1.ContainerStatus{}, false
}
