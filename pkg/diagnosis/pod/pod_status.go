package pod

import (
	"fmt"
	"reflect"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"
	"github.com/baizeai/kcover/pkg/runner"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var _ runner.Runner = (*diagnostic)(nil)
var _ runner.Runner = (*diag)(nil)
var _ diagnosis.Diagnostic = (*diag)(nil)

type diagnostic struct {
	diagnostics []diagnosis.Diagnostic
	eventSink   events.Sink
}

// NewDiagnostic 创建由 pod 诊断管理的诊断器，目前只支持 Pod 诊断。
func NewDiagnostic(cli kubernetes.Interface, sink events.Sink) (runner.Runner, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink can not be nil")
	}

	diag, err := newPodStatusDiagnosis(cli)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod status collector: %w", err)
	}

	return &diagnostic{
		diagnostics: []diagnosis.Diagnostic{diag},
		eventSink:   sink,
	}, nil
}

type diag struct {
	client  kubernetes.Interface
	eventCh chan events.Event
	stopCh  chan struct{}
}

func newPodStatusDiagnosis(cli kubernetes.Interface) (diagnosis.Diagnostic, error) {
	return &diag{
		client:  cli,
		eventCh: make(chan events.Event),
		stopCh:  make(chan struct{}),
	}, nil
}

func (p *diag) onPodUpdate(oldPod, newPod *corev1.Pod) {
	if !shouldCheckPodUpdate(oldPod, newPod) {
		return
	}

	for _, event := range podEvents(newPod) {
		p.eventCh <- event
	}
}

func shouldCheckPodUpdate(oldPod, newPod *corev1.Pod) bool {
	if oldPod == nil {
		return true
	}

	return !reflect.DeepEqual(oldPod.Status.ContainerStatuses, newPod.Status.ContainerStatuses) ||
		!reflect.DeepEqual(oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses)
}

func containerEvents(pod *corev1.Pod) []events.Event {
	ee := make([]events.Event, 0)
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			if cs.State.Terminated.Reason == "Error" {
				ee = append(ee, events.Event{
					ResourceType: events.Pod,
					Namespace:    pod.Namespace,
					Name:         pod.Name,
					EventType:    events.Error,
					Message:      fmt.Sprintf("container %s terminated with error: %s, exit code: %d", cs.Name, cs.State.Terminated.Message, cs.State.Terminated.ExitCode),
				})
			}
		}
	}

	return ee
}

func podEvents(pod *corev1.Pod) []events.Event {
	if !shouldHandlePod(pod) {
		return nil
	}

	return containerEvents(pod)
}

func shouldHandlePod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	return pod.Labels[constants.EnabledRecoveryLabel] != ""
}

func (p *diag) Start() error {
	err := kube.WatchPods(p.client, p.stopCh, kube.PodHandlerFuncs{
		AddFunc: func(newPod *corev1.Pod) {
			p.onPodUpdate(nil, newPod)
		},
		UpdateFunc: func(oldPod, newPod *corev1.Pod) {
			p.onPodUpdate(oldPod, newPod)
		},
	})
	if err != nil {
		return err
	}

	klog.Info("the podStatusCollector is started")
	return nil
}

func (p *diag) Stop() {
	close(p.stopCh)
	close(p.eventCh)
}

func (p *diag) EventChan() <-chan events.Event {
	return p.eventCh
}

func (c *diagnostic) Start() error {
	for _, d := range c.diagnostics {
		if err := d.Start(); err != nil {
			return err
		}
	}
	for _, d := range c.diagnostics {
		go func(diag diagnosis.Diagnostic) {
			for event := range diag.EventChan() {
				if err := c.eventSink.RecordEvent(event); err != nil {
					klog.Errorf("failed to record event of %T: %v", diag, err)
				}
			}
		}(d)
	}

	klog.Info("the pod diagnostic is started")
	return nil
}

func (c *diagnostic) Stop() {
	for _, d := range c.diagnostics {
		d.Stop()
	}
}
