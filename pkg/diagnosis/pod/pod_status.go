package pod

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/diagnosis"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/preflight"
	"github.com/baizeai/kcover/pkg/runner"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var _ runner.Runner = (*podStatusCollector)(nil)
var _ diagnosis.Diagnostic = (*podStatusCollector)(nil)

const preflightLabel = "kcover.io/preflight"

type podStatusCollector struct {
	client  kubernetes.Interface
	eventCh chan events.Event
	stopCh  chan struct{}
}

func newPodStatusCollector(cli kubernetes.Interface) (diagnosis.Diagnostic, error) {
	return &podStatusCollector{
		client:  cli,
		eventCh: make(chan events.Event),
		stopCh:  make(chan struct{}),
	}, nil
}

func (p *podStatusCollector) onPodUpdate(oldPod, newPod *corev1.Pod) {
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

func preflightEvents(pod *corev1.Pod) []events.Event {
	ee := make([]events.Event, 0)
	for _, cs := range pod.Status.InitContainerStatuses {
		terminated := cs.State.Terminated
		if terminated == nil || strings.TrimSpace(terminated.Message) == "" {
			continue
		}

		var report preflight.Report
		if err := json.Unmarshal([]byte(strings.TrimSpace(terminated.Message)), &report); err != nil {
			continue
		}

		if report.Result == preflight.CheckResultFail {
			ee = append(ee, events.Event{
				ResourceType: events.Pod,
				Namespace:    pod.Namespace,
				Name:         pod.Name,
				EventType:    events.Error,
				Message:      fmt.Sprintf("init container %s preflight failed on node %s", cs.Name, pod.Spec.NodeName),
			})
		}

		if report.Checks.NodeCheck == preflight.CheckResultFail && pod.Spec.NodeName != "" {
			ee = append(ee, events.Event{
				ResourceType: events.Node,
				Name:         pod.Spec.NodeName,
				EventType:    events.Error,
				Message:      fmt.Sprintf("pod %s/%s init container %s reported node preflight failure", pod.Namespace, pod.Name, cs.Name),
			})
		}

		for nodeName, result := range report.Checks.Network.Target {
			if result != preflight.CheckResultFail {
				continue
			}

			ee = append(ee, events.Event{
				ResourceType: events.Node,
				Name:         nodeName,
				EventType:    events.Error,
				Message:      fmt.Sprintf("pod %s/%s init container %s reported network preflight failure to node %s", pod.Namespace, pod.Name, cs.Name, nodeName),
			})
		}
	}

	return ee
}

func podEvents(pod *corev1.Pod) []events.Event {
	if !shouldHandlePod(pod) {
		return nil
	}

	ee := make([]events.Event, 0)
	ee = append(ee, containerEvents(pod)...)
	ee = append(ee, preflightEvents(pod)...)
	return ee
}

func shouldHandlePod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	return pod.Labels[constants.EnabledRecoveryLabel] != "" ||
		pod.Labels[preflightLabel] != ""
}

func (p *podStatusCollector) Start() error {
	factory := informers.NewSharedInformerFactory(p.client, time.Minute)
	informer := factory.Core().V1().Pods().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			newPod := obj.(*corev1.Pod)
			p.onPodUpdate(nil, newPod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod := newObj.(*corev1.Pod)
			oldPod := oldObj.(*corev1.Pod)
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				return
			}

			p.onPodUpdate(oldPod, newPod)
		},
	})
	if err != nil {
		return err
	}

	go informer.Run(p.stopCh)
	klog.Info("the podStatusCollector is started")
	return nil
}

func (p *podStatusCollector) Stop() {
	close(p.stopCh)
	close(p.eventCh)
}

func (p *podStatusCollector) EventChan() <-chan events.Event {
	return p.eventCh
}
