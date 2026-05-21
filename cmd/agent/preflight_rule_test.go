package main

import (
	"path/filepath"
	"testing"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/preflight"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestShouldHandlePreflightPodUpdate(t *testing.T) {
	t.Parallel()

	oldPod := &corev1.Pod{
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:  "preflight",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
			}},
		},
	}

	newPod := oldPod.DeepCopy()
	newPod.ObjectMeta.Labels = map[string]string{constants.PreflightLabel: constants.True}
	newPod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
		Name:  "preflight",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
	}}

	if !shouldHandlePodUpdate(oldPod, newPod) {
		t.Fatal("shouldHandlePreflightPodUpdate(oldPod, newPod) = false, want true")
	}

	unchangedFailed := newPod.DeepCopy()
	if shouldHandlePodUpdate(newPod, unchangedFailed) {
		t.Fatal("shouldHandlePreflightPodUpdate(newPod, unchangedFailed) = true, want false")
	}
}

func TestIsPreflightFailed(t *testing.T) {
	t.Parallel()

	oldStatuses := []corev1.ContainerStatus{{
		Name:  "preflight",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}
	newStatuses := []corev1.ContainerStatus{{
		Name:  "preflight",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
	}}

	if !isPreflightFailed(oldStatuses, newStatuses) {
		t.Fatal("isPreflightFailed(oldStatuses, newStatuses) = false, want true")
	}
}

func TestIsPreflightFailedRequiresExactPreflightName(t *testing.T) {
	t.Parallel()

	oldStatuses := []corev1.ContainerStatus{{
		Name:  "other-init",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}
	newStatuses := []corev1.ContainerStatus{{
		Name:  "other-init",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
	}}

	if isPreflightFailed(oldStatuses, newStatuses) {
		t.Fatal("isPreflightFailed(oldStatuses, newStatuses) = true, want false")
	}
}

func TestShouldHandlePreflightPodUpdateRequiresLabel(t *testing.T) {
	t.Parallel()

	oldPod := &corev1.Pod{}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name:  "preflight",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
		}}},
	}

	if shouldHandlePodUpdate(oldPod, newPod) {
		t.Fatal("shouldHandlePreflightPodUpdate(oldPod, newPod) = true, want false")
	}
}

func TestPreflightWorkloadNamePrefersLeaderWorkerSetLabel(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			constants.LeaderWorkerSetNameLabel: "lws-job",
			constants.BatchJobNameLabel:        "batch-job-node-0",
		}},
	}

	if got := preflightWorkloadName(pod); got != "lws-job" {
		t.Fatalf("preflightWorkloadName(pod) = %q, want lws-job", got)
	}
}

func TestPreflightWorkloadNameFallsBackToBatchJobLabel(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			constants.BatchJobNameLabel: "batch-job-node-0",
		}},
	}

	if got := preflightWorkloadName(pod); got != "batch-job-node-0" {
		t.Fatalf("preflightWorkloadName(pod) = %q, want batch-job-node-0", got)
	}
}

func TestPreflightReportNameUsesWorkloadNameAndRank(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "test2-worker-1",
		Labels: map[string]string{
			constants.BatchJobNameLabel: "test2",
		},
		Annotations: map[string]string{
			constants.BatchJobCompletionIndexAnnotation: "1",
		},
	}}

	reportName, ok := preflightReportName(pod, "test2")
	if !ok {
		t.Fatal("preflightReportName(pod, test2) = false, want true")
	}
	if reportName != "test2-1" {
		t.Fatalf("preflightReportName(pod, test2) = %q, want test2-1", reportName)
	}

	path := preflight.ReportPath("/var/lib/kcover/preflight", "baize-test", reportName)
	want := filepath.Join("/var/lib/kcover/preflight", "baize-test", "test2-1.json")
	if path != want {
		t.Fatalf("ReportPath(...) = %q, want %q", path, want)
	}
}

func TestPreflightReportNameRejectsUnmatchedPodName(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "plain-pod-name",
		Labels: map[string]string{
			constants.BatchJobNameLabel: "test2",
		},
	}}

	if reportName, ok := preflightReportName(pod, "test2"); ok {
		t.Fatalf("preflightReportName(pod, test2) = (%q, true), want false", reportName)
	}
}

func TestPreflightReportNameRejectsInvalidBatchCompletionIndex(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "test2-worker-abc",
		Labels: map[string]string{
			constants.BatchJobNameLabel: "test2",
		},
		Annotations: map[string]string{
			constants.BatchJobCompletionIndexAnnotation: "abc",
		},
	}}

	if reportName, ok := preflightReportName(pod, "test2"); ok {
		t.Fatalf("preflightReportName(pod, test2) = (%q, true), want false", reportName)
	}
}

func TestPreflightReportNameLWSFallsBackToPodNameRank(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "lws-job-worker-3",
		Labels: map[string]string{
			constants.LeaderWorkerSetNameLabel: "lws-job",
		},
	}}

	reportName, ok := preflightReportName(pod, "lws-job")
	if !ok {
		t.Fatal("preflightReportName(pod, lws-job) = false, want true")
	}
	if reportName != "lws-job-3" {
		t.Fatalf("preflightReportName(pod, lws-job) = %q, want lws-job-3", reportName)
	}
}
