package main

import (
	"fmt"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"
	"github.com/baizeai/kcover/pkg/preflight"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const preflightReportDir = "/var/lib/kcover/preflight"

type podWatcher struct {
	client  kubernetes.Interface
	sink    events.Sink
	stopCh  chan struct{}
	baseDir string
}

func newPodWatcher(cli kubernetes.Interface, sink events.Sink) (*podWatcher, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink can not be nil")
	}

	return &podWatcher{
		client:  cli,
		sink:    sink,
		stopCh:  make(chan struct{}),
		baseDir: preflightReportDir,
	}, nil
}

func (w *podWatcher) Start() error {
	err := kube.WatchPods(w.client, w.stopCh, kube.PodHandlerFuncs{
		UpdateFunc: func(oldPod, newPod *corev1.Pod) {
			w.onPodUpdate(oldPod, newPod)
		},
	})
	if err != nil {
		return fmt.Errorf("add pod update handler: %w", err)
	}

	klog.Info("preflight pod watcher started")
	return nil
}

func (w *podWatcher) Stop() {
	close(w.stopCh)
}

func (w *podWatcher) onPodUpdate(oldPod, newPod *corev1.Pod) {
	if !shouldHandlePodUpdate(oldPod, newPod) {
		return
	}

	report, err := preflight.LoadReportFile(w.baseDir, newPod.Namespace, newPod.Name)
	if err != nil {
		klog.Errorf("load preflight report for pod %s/%s: %v", newPod.Namespace, newPod.Name, err)
		return
	}

	for _, event := range preflight.NodeEvents(newPod.Namespace, newPod.Name, report) {
		if err := w.sink.RecordEvent(event); err != nil {
			klog.Errorf("record preflight event for pod %s/%s and node %s: %v", newPod.Namespace, newPod.Name, event.Name, err)
		}
	}
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
