package main

import (
	"testing"

	"github.com/baizeai/kcover/pkg/constants"

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

func TestHasNewFailedInitContainer(t *testing.T) {
	t.Parallel()

	oldStatuses := []corev1.ContainerStatus{{
		Name:  "preflight-a",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}
	newStatuses := []corev1.ContainerStatus{
		{
			Name:  "preflight-a",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
		},
		{
			Name:  "preflight-b",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
		},
	}

	if !hasNewFailedInitContainer(oldStatuses, newStatuses) {
		t.Fatal("hasNewFailedInitContainer(oldStatuses, newStatuses) = false, want true")
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
