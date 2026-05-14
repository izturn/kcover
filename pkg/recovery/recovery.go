package recovery

import (
	"context"
	"errors"
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
	eventSink          events.Sink
	stopCh             chan struct{}
	restartDuration    time.Duration
	restarts           *ttlcache.Cache[string, time.Time]
	slowNodeAggregator *preflight.SlowNodeAggregator
}

type ControllerOptions struct {
	PreflightReportCollectionTimeout time.Duration
}

// TODO: Re-enable after preflight rollout validation is completed.
const preflightFeatureEnabled = false

func NewController(cli kubernetes.Interface, stream events.Stream, opts ...ControllerOptions) *RecoveryController {
	var aggregator *preflight.SlowNodeAggregator
	if preflightFeatureEnabled {
		preflightConfig, err := preflight.LoadConfig(context.Background(), cli, kube.CurrentNamespace())
		if err != nil {
			klog.Warningf("load preflight config: %v; using defaults", err)
			preflightConfig = preflight.DefaultConfig()
		}
		if len(opts) > 0 && opts[0].PreflightReportCollectionTimeout > 0 {
			preflightConfig.ReportCollectionTimeout = opts[0].PreflightReportCollectionTimeout
		}
		preflightConfig = preflightConfig.Normalize()
		aggregator = preflight.NewSlowNodeAggregator(preflightConfig)
	} else {
		klog.Info("preflight feature is temporarily disabled")
	}

	return &RecoveryController{
		client:             cli,
		eventStream:        stream,
		eventSink:          events.NewKubeEventSink(cli),
		stopCh:             make(chan struct{}),
		restartDuration:    time.Second * 30,
		restarts:           ttlcache.New[string, time.Time](),
		slowNodeAggregator: aggregator,
	}
}

func (r *RecoveryController) reportPreflightAggregationTimeout(timeoutErr preflight.ReportCollectionTimeoutError) {
	if r.eventSink == nil {
		return
	}
	anchorNode := timeoutErr.AnchorNodeName()
	if anchorNode == "" {
		klog.V(2).Infof("skip warning event for expired preflight aggregation %s/%s: no reported nodes", timeoutErr.Namespace, timeoutErr.JobName)
		return
	}

	message := fmt.Sprintf(
		"preflight aggregation for job %s timed out after %s: received %d/%d reports; reported nodes=%v",
		timeoutErr.JobName,
		timeoutErr.Timeout,
		timeoutErr.ReceivedReports,
		timeoutErr.ExpectedReports,
		timeoutErr.ReportedNodes,
	)
	if err := r.eventSink.RecordEvent(events.Event{
		ResourceType: events.Node,
		Namespace:    timeoutErr.Namespace,
		Name:         anchorNode,
		EventType:    events.Warning,
		Message:      message,
	}); err != nil {
		klog.Errorf("record preflight aggregation timeout warning for %s/%s: %v", timeoutErr.Namespace, timeoutErr.JobName, err)
	}
}

func (r *RecoveryController) onPodError(namespace, name string) {
	klog.V(2).Infof("start handling pod error ns=%s pod=%s", namespace, name)
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
		klog.V(4).Infof("skip recovery for pod %s/%s: pod and owner job have no recovery label", namespace, name)
		return
	}

	jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]
	if !ok {
		klog.V(2).Infof("skip recovery for pod %s/%s: missing job label", namespace, name)
		return
	}

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		klog.V(2).Infof("skip recovery for pod %s/%s: restartPolicy is Never", namespace, name)
		return
	}
	if !r.allowJobRestart(namespace, jobLabel) {
		return
	}

	klog.V(2).Infof("trigger pod recovery restart for ns=%s job=%s from pod=%s", namespace, jobLabel, name)
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
		klog.V(2).Infof("skip restart for job %s/%s: last restarted at %v, retry window %v", namespace, jobLabel, restartedAt.Value(), r.restartDuration)
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
	klog.V(2).Infof("start handling node error node=%s", name)
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
	klog.V(2).Infof(
		"start handling preflight report event ns=%s node=%s annotations=%v messageBytes=%d",
		namespace,
		e.Name,
		e.Annotations,
		len(e.Message),
	)
	if r.slowNodeAggregator == nil {
		klog.V(2).Info("skip preflight report: collector is nil")
		return
	}

	jobName := e.Annotations[constants.KubeflowJobLabel]
	if jobName == "" {
		klog.V(2).Infof("skip preflight report %s/%s: missing %s", namespace, e.Name, constants.KubeflowJobLabel)
		return
	}

	ready, slowNodes, err := r.slowNodeAggregator.AddReport(namespace, jobName, e.Message)
	if err != nil {
		var timeoutErr preflight.ReportCollectionTimeoutError
		if errors.As(err, &timeoutErr) {
			r.reportPreflightAggregationTimeout(timeoutErr)
			klog.Errorf("expired stale preflight aggregation before processing %s/%s: %v", namespace, jobName, err)
		}
		klog.Errorf("aggregate preflight report for %s/%s: %v", namespace, jobName, err)
		return
	}

	if !ready {
		klog.V(2).Infof("received preflight report for %s/%s, waiting for more", namespace, jobName)
		return
	}

	if len(slowNodes) == 0 {
		klog.Infof("preflight report for %s/%s finished without slow nodes", namespace, jobName)
		return
	}

	for _, nodeName := range slowNodes {
		klog.V(2).Infof("preflight marked slow node node=%s for ns=%s job=%s", nodeName, namespace, jobName)
		r.ensureNodeUnschedulable(nodeName)
	}
}

func (r *RecoveryController) sweepExpiredPreflightReports() {
	if r.slowNodeAggregator == nil {
		return
	}
	for _, err := range r.slowNodeAggregator.ExpireStale() {
		r.reportPreflightAggregationTimeout(err)
		klog.Errorf("expire stale preflight aggregation: %v", err)
	}
}

func (r *RecoveryController) onEvent(e events.Event) {
	klog.V(4).Infof("recovery controller received event: %+v", e)
	switch e.ResourceType {
	case events.Pod:
		if e.EventType == events.Error {
			klog.V(2).Infof("dispatch event to pod recovery ns=%s pod=%s", e.Namespace, e.Name)
			r.onPodError(e.Namespace, e.Name)
		}
	case events.Node:
		if e.Annotations[constants.PreflightReportAnnotation] == constants.True {
			if !preflightFeatureEnabled {
				klog.V(2).Infof("ignore preflight event for node %s: preflight feature is disabled", e.Name)
				return
			}
			klog.V(2).Infof("dispatch event to preflight aggregator ns=%s node=%s", e.Namespace, e.Name)
			r.onPreflightReport(e.Namespace, e)
			return
		}
		klog.V(2).Infof("dispatch event to node recovery node=%s", e.Name)
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
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return // TODO: wait for event stream finish?

			case <-ticker.C:
				r.sweepExpiredPreflightReports()

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
