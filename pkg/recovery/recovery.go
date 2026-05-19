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
	client                 kubernetes.Interface
	eventStream            events.Stream
	eventSink              events.Sink
	stopCh                 chan struct{}
	preflightSweepInterval time.Duration
	restartDuration        time.Duration
	restarts               *ttlcache.Cache[string, time.Time]
	slowNodeAgg            *preflight.SlowNodeAggregator
}

const DefaultPreflightSweepInterval = time.Minute

func NewController(cli kubernetes.Interface, stream events.Stream, preflightReportCollectionTimeout, preflightSweepInterval time.Duration) *RecoveryController {
	slowNodeAgg := preflight.NewSlowNodeAggregator(preflightReportCollectionTimeout)
	if preflightSweepInterval <= 0 {
		preflightSweepInterval = DefaultPreflightSweepInterval
	}

	return &RecoveryController{
		client:                 cli,
		eventStream:            stream,
		eventSink:              events.NewKubeEventSink(cli),
		stopCh:                 make(chan struct{}),
		preflightSweepInterval: preflightSweepInterval,
		restartDuration:        time.Second * 30,
		restarts:               ttlcache.New[string, time.Time](),
		slowNodeAgg:            slowNodeAgg,
	}
}

func (r *RecoveryController) handlePreflightTimeout(timeoutErr preflight.WorkloadTimeoutError) {
	anchorNode := timeoutErr.FirstReportedNode()
	if anchorNode == "" {
		klog.V(2).InfoS("skip preflight aggregation timeout report", "namespace", timeoutErr.Namespace, "workload", timeoutErr.WorkloadName, "reason", "no reported nodes")
		return
	}

	klog.ErrorS(nil, "preflight aggregation timeout", "node", anchorNode, "namespace", timeoutErr.Namespace, "workload", timeoutErr.WorkloadName, "timeout", timeoutErr.Timeout, "receivedReports", timeoutErr.ReceivedReports, "expectedReports", timeoutErr.ExpectedReports, "reportedNodes", timeoutErr.ReportedNodes)
}

func (r *RecoveryController) onPodError(namespace, name string) {
	klog.V(2).InfoS("start handling pod error", "namespace", namespace, "pod", name)
	pod, err := r.client.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "get pod failed", "namespace", namespace, "pod", name)
		return
	}

	recoveryEnabled, err := r.isRecoveryEnabledForPod(pod)
	if err != nil {
		klog.ErrorS(err, "get recovery labels failed", "namespace", namespace, "pod", name)
		return
	}
	if !recoveryEnabled {
		klog.V(4).InfoS("skip recovery for pod", "namespace", namespace, "pod", name, "reason", "pod and owner job have no recovery label")
		return
	}

	jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]
	if !ok {
		klog.V(2).InfoS("skip recovery for pod", "namespace", namespace, "pod", name, "reason", "missing job label")
		return
	}

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		klog.V(2).InfoS("skip recovery for pod", "namespace", namespace, "pod", name, "reason", "restartPolicy is Never")
		return
	}
	if !r.allowJobRestart(namespace, jobLabel) {
		return
	}

	klog.V(2).InfoS("trigger pod recovery restart", "namespace", namespace, "job", jobLabel, "pod", name)
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
		klog.V(2).InfoS("skip restart for job", "namespace", namespace, "job", jobLabel, "lastRestartedAt", restartedAt.Value(), "retryWindow", r.restartDuration)
		return false
	}

	now := time.Now()
	r.restarts.Set(key, now, r.restartDuration) // only restart once within restartDuration
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
		klog.ErrorS(err, "restart job failed", "namespace", namespace, "job", name)
	} else {
		klog.InfoS("restarted job", "namespace", namespace, "job", name)
	}
}

type nsName struct {
	ns   string
	name string
}

func (r *RecoveryController) ensureNodeUnschedulable(name string) bool {
	if err := kube.TaintNodeUnschedulable(context.Background(), r.client, name); err != nil {
		klog.ErrorS(err, "mark node unschedulable failed", "node", name)
		return false
	}

	klog.InfoS("marked node unschedulable", "node", name, "taint", "NoSchedule")
	return true
}

func (r *RecoveryController) onNodeError(name string) {
	klog.V(2).InfoS("start handling node error", "node", name)
	node, err := r.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "get node failed", "node", name)
		return
	}

	if node.Spec.Unschedulable {
		r.ensureNodeUnschedulable(name)
		return
	}

	jobs, err := r.listJobsOnNode(name)
	if err != nil {
		klog.ErrorS(err, "list jobs on node failed", "node", name)
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
	klog.V(2).InfoS("start handling preflight report event", "namespace", namespace, "node", e.Name, "annotations", e.Annotations, "messageBytes", len(e.Message))

	if r.slowNodeAgg == nil {
		klog.V(2).InfoS("skip preflight report", "reason", "collector is nil")
		return
	}

	workloadName := e.Annotations[constants.PreflightWorkloadAnnotation]
	if workloadName == "" {
		klog.V(2).InfoS("skip preflight report", "namespace", namespace, "node", e.Name, "missingAnnotation", constants.PreflightWorkloadAnnotation)
		return
	}

	ready, slowNodes, err := r.slowNodeAgg.AddReport(namespace, workloadName, e.Message)
	if err != nil {
		var timeoutErr preflight.WorkloadTimeoutError
		if errors.As(err, &timeoutErr) {
			r.handlePreflightTimeout(timeoutErr)
			return
		}
		klog.ErrorS(err, "aggregate preflight report failed", "namespace", namespace, "workload", workloadName)
		return
	}

	if !ready {
		klog.V(2).InfoS("received preflight report", "namespace", namespace, "workload", workloadName, "state", "waiting")
		return
	}

	if len(slowNodes) == 0 {
		klog.InfoS("preflight report finished without slow nodes", "namespace", namespace, "workload", workloadName)
		return
	}

	for _, node := range slowNodes {
		klog.V(2).InfoS("preflight marked slow node", "node", node, "namespace", namespace, "workload", workloadName)
		r.ensureNodeUnschedulable(node)
	}
}

func (r *RecoveryController) sweepExpiredPreflightReports() {
	if r.slowNodeAgg == nil {
		return
	}
	for _, err := range r.slowNodeAgg.ExpireTimedOutWorkloads() {
		r.handlePreflightTimeout(err)
		klog.ErrorS(err, "expire stale preflight aggregation failed")
	}
}

func (r *RecoveryController) onEvent(e events.Event) {
	klog.V(4).InfoS("recovery controller received event", "event", e)
	switch e.ResourceType {
	case events.Pod:
		if e.EventType == events.Error {
			klog.V(2).InfoS("dispatch event to pod recovery", "namespace", e.Namespace, "pod", e.Name)
			r.onPodError(e.Namespace, e.Name)
		}
	case events.Node:
		if events.IsPreflightEvent(e.Annotations) {
			klog.V(2).InfoS("dispatch event to preflight aggregator", "namespace", e.Namespace, "node", e.Name)
			r.onPreflightReport(e.Namespace, e)
			return
		}
		klog.V(2).InfoS("dispatch event to node recovery", "node", e.Name)
		r.onNodeError(e.Name)
	default:
		klog.ErrorS(nil, "unsupported event resource type", "resourceType", e.ResourceType)
	}
}

func (r *RecoveryController) Start() error {
	if r.eventStream == nil {
		return fmt.Errorf("event stream is nil")
	}
	go func() {
		ticker := time.NewTicker(r.preflightSweepInterval)
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

	klog.InfoS("recovery controller started")
	return nil
}

func (r *RecoveryController) Stop() {
	close(r.stopCh)
}
