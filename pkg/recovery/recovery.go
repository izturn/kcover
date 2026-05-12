package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"
	"github.com/baizeai/kcover/pkg/preflight"

	"github.com/jellydator/ttlcache/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type RecoveryController struct {
	client             kubernetes.Interface
	eventStream        events.Stream
	stopCh             chan struct{}
	restartDuration    time.Duration
	restarts           *ttlcache.Cache[string, time.Time]
	preflightCollector *preflight.JobCollector
}

func NewController(cli kubernetes.Interface, stream events.Stream) *RecoveryController {
	preflightConfig, err := preflight.LoadConfig(context.Background(), cli, kube.CurrentNamespace())
	if err != nil {
		klog.Warningf("load preflight config: %v; using defaults", err)
		preflightConfig = preflight.DefaultConfig()
	}

	return &RecoveryController{
		client:             cli,
		eventStream:        stream,
		stopCh:             make(chan struct{}),
		restartDuration:    time.Second * 30,
		restarts:           ttlcache.New[string, time.Time](),
		preflightCollector: preflight.NewJobCollector(preflightConfig),
	}
}

func (r *RecoveryController) onPodError(namespace, name string) {
	pod, err := r.client.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("get pod %s/%s: %v", namespace, name, err)
		return
	}

	recoveryEnabled, err := r.isRecoveryEnabledForPod(pod)
	if err != nil {
		klog.Errorf("get recovery labels for pod %s/%s: %v", namespace, name, err)
		return
	}
	if !recoveryEnabled {
		klog.Infof("skip recovery for pod %s/%s: pod and owner job have no recovery label", namespace, name)
		return
	}

	jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]
	if !ok {
		klog.Warningf("skip recovery for pod %s/%s: missing job label", namespace, name)
		return
	}

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		klog.Warningf("skip recovery for pod %s/%s: restartPolicy is Never", namespace, name)
		return
	}
	if !r.allowJobRestart(namespace, jobLabel) {
		return
	}

	r.restartJob(context.Background(), namespace, jobLabel)
}

func (r *RecoveryController) isRecoveryEnabledForPod(pod *corev1.Pod) (bool, error) {
	if pod.Labels[constants.EnabledRecoveryLabel] == constants.True {
		return true, nil
	}

	labels, err := getPodRelatedJobLabels(r.client, pod)
	if err != nil {
		return false, err
	}

	return labels[constants.EnabledRecoveryLabel] == constants.True, nil
}

func (r *RecoveryController) allowJobRestart(namespace, jobLabel string) bool {
	key := fmt.Sprintf("%s/%s", namespace, jobLabel)
	restartedAt := r.restarts.Get(key)
	if restartedAt != nil {
		klog.Infof("skip restart for job %s/%s: last restarted at %v, retry window %v", namespace, jobLabel, restartedAt.Value(), r.restartDuration)
		return false
	}

	now := time.Now()
	r.restarts.Set(key, now, r.restartDuration) // only restart once in 60 seconds
	go func() {
		<-time.After(r.restartDuration - time.Second)
		r.restarts.Delete(key)
	}()

	return true
}

func (r *RecoveryController) restartJob(ctx context.Context, namespace, name string) {
	err := r.client.CoreV1().Pods(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", constants.KubeflowJobLabel, name),
	})
	if err != nil {
		klog.Errorf("restart job %s/%s: %v", namespace, name, err)
	} else {
		klog.Infof("restarted job %s/%s", namespace, name)
	}
}

type nsName struct {
	ns   string
	name string
}

func (r *RecoveryController) ensureNodeUnschedulable(name string) bool {
	if err := kube.TaintNodeUnschedulable(context.Background(), r.client, name); err != nil {
		klog.Errorf("mark node %s unschedulable: %v", name, err)
		return false
	}

	klog.Infof("marked node %s unschedulable with no-schedule taint", name)
	return true
}

func (r *RecoveryController) onNodeError(name string) {
	node, err := r.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("get node %s: %v", name, err)
		return
	}

	if node.Spec.Unschedulable {
		r.ensureNodeUnschedulable(name)
		return
	}

	jobs, err := r.listJobsOnNode(name)
	if err != nil {
		klog.Errorf("list jobs on node %s: %v", name, err)
		return
	}

	for _, job := range jobs {
		r.onPodError(job.ns, job.name)
	}

	r.ensureNodeUnschedulable(name)
}

func (r *RecoveryController) listJobsOnNode(nodeName string) ([]nsName, error) {
	// TODO: traner v2 & lws?
	pods, err := r.client.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		LabelSelector: constants.KubeflowJobLabel,
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return nil, err
	}

	jobs := make(map[nsName]struct{})
	for _, pod := range pods.Items {
		jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]
		if !ok {
			continue
		}

		jobs[nsName{ns: pod.Namespace, name: jobLabel}] = struct{}{}
	}

	items := make([]nsName, 0, len(jobs))
	for job := range jobs {
		items = append(items, job)
	}

	return items, nil
}

func (r *RecoveryController) onPreflightReport(namespace string, e events.Event) {
	if r.preflightCollector == nil {
		klog.Warning("skip preflight report: collector is nil")
		return
	}

	jobName := e.Annotations[constants.KubeflowJobLabel]
	if jobName == "" {
		klog.Warningf("skip preflight report %s/%s: missing %s", namespace, e.Name, constants.KubeflowJobLabel)
		return
	}

	ready, badNodes, err := r.preflightCollector.Add(namespace, jobName, e.Message)
	if err != nil {
		klog.Errorf("aggregate preflight report for %s/%s: %v", namespace, jobName, err)
		return
	}

	if !ready {
		klog.Infof("received preflight report for %s/%s, waiting for more", namespace, jobName)
		return
	}

	if len(badNodes) == 0 {
		klog.Infof("preflight report for %s/%s finished without bad nodes", namespace, jobName)
		return
	}

	for _, nodeName := range badNodes {
		r.ensureNodeUnschedulable(nodeName)
	}
}

func (r *RecoveryController) onEvent(e events.Event) {
	klog.Infof("recovery controller received event: %+v", e)
	switch e.ResourceType {
	case events.Pod:
		if e.EventType == events.Error {
			r.onPodError(e.Namespace, e.Name)
		}
	case events.Node:
		if e.Annotations[constants.PreflightReportAnnotation] == constants.True {
			r.onPreflightReport(e.Namespace, e)
			return
		}
		r.onNodeError(e.Name)
	default:
		klog.Errorf("unsupported event resource type: %s", e.ResourceType)
	}
}

func (r *RecoveryController) Start() error {
	if r.eventStream == nil {
		return fmt.Errorf("event stream is nil")
	}
	go func() {
		for {
			select {
			case <-r.stopCh:
				return // TODO: wait for event stream finish?

			case e := <-r.eventStream.EventChan():
				r.onEvent(e)
			}
		}

	}()

	klog.Info("recovery controller started")
	return nil
}

func (r *RecoveryController) Stop() {
	close(r.stopCh)
}
