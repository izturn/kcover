package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/baizeai/kcover/pkg/constants"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"

	"github.com/jellydator/ttlcache/v3"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type recoveryController struct {
	client          kubernetes.Interface
	eventStream     events.Stream
	stopCh          chan struct{}
	restartDuration time.Duration
	restarts        *ttlcache.Cache[string, time.Time]
}

func NewRecoveryController(cli kubernetes.Interface, stream events.Stream) *recoveryController {
	return &recoveryController{
		client:          cli,
		eventStream:     stream,
		stopCh:          make(chan struct{}),
		restartDuration: time.Second * 30,
		restarts:        ttlcache.New[string, time.Time](),
	}
}

func (r *recoveryController) onPodError(namespace, name string) {
	pod, err := r.client.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("get pod %s/%s error events error: %v", namespace, name, err)
		return
	}
	if pod.Labels[constants.EnabledRecoveryLabel] != constants.True {
		ls, err := getPodRelatedJobLabels(r.client, pod)
		if err != nil {
			klog.Errorf("get pod %s/%s related job labels error: %v", namespace, name, err)
			return
		}
		if ls[constants.EnabledRecoveryLabel] != constants.True {
			klog.Infof("pod %s/%s or its owner job has no recovery label", namespace, name)
			return
		}
	}

	jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]
	if !ok {
		klog.Warningf("pod %s/%s has no job label", namespace, name)
		return
	}

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		klog.Warningf("pod %s/%s has RestartPolicyNever, will not restart", namespace, name)
		return
	}
	key := fmt.Sprintf("%s/%s", namespace, jobLabel)
	tv := r.restarts.Get(key)
	if tv != nil {
		klog.Infof("job %s/%s has been restarted at %v, will not restart again in %v", namespace, jobLabel, tv.Value(), r.restartDuration)
		return
	}
	now := time.Now()
	r.restarts.Set(key, now, r.restartDuration) // only restart once in 60 seconds
	r.restartJob(context.Background(), namespace, jobLabel)
	go func() {
		<-time.After(r.restartDuration - time.Second)
		r.restarts.Delete(key) //
	}()

}

func (r *recoveryController) restartJob(ctx context.Context, namespace, name string) {
	err := r.client.CoreV1().Pods(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", constants.KubeflowJobLabel, name),
	})
	if err != nil {
		klog.Errorf("restart job %s/%s error: %v", namespace, name, err)
	} else {
		klog.Infof("restart job %s/%s successfully", namespace, name)
	}
}

type nsName struct {
	ns   string
	name string
}

func (r *recoveryController) ensureNodeUnschedulable(name string) bool {
	if err := kube.TaintNodeUnschedulable(context.Background(), r.client, name); err != nil {
		klog.Errorf("ensure node %s is unschedulable error: %v", name, err)
		return false
	}

	klog.Infof("ensured node %s is unschedulable and carries the no-schedule taint", name)
	return true
}

func (r *recoveryController) onNodeError(name string) {
	node, err := r.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("get node %s error: %v", name, err)
		return
	}
	if node.Spec.Unschedulable {
		r.ensureNodeUnschedulable(name)
		return
	}
	// query jobs
	pods, err := r.client.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		LabelSelector: constants.KubeflowJobLabel,
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", name),
	})
	if err != nil {
		klog.Errorf("fetch pods list for node %s error: %v", node, err)
		return
	}
	jobs := map[nsName]struct{}{}
	lo.ForEach(pods.Items, func(pod corev1.Pod, index int) {
		if jobLabel, ok := pod.Labels[constants.KubeflowJobLabel]; !ok {
			return
		} else {
			jobs[nsName{
				ns:   pod.Namespace,
				name: jobLabel,
			}] = struct{}{}
		}
	})
	lo.ForEach(lo.Keys(jobs), func(item nsName, index int) {
		r.onPodError(item.ns, item.name)
	})
	r.ensureNodeUnschedulable(name)
}

func (r *recoveryController) onEvent(e events.Event) {
	klog.Infof("recover controller received event: %+v", e)
	switch e.ResourceType {
	case events.Pod:
		if e.EventType == events.Error {
			r.onPodError(e.Namespace, e.Name)
		}
	case events.Node:
		r.onNodeError(e.Name)
	default:
		klog.Errorf("unsupported target type: %s", e.ResourceType)
	}
}

func (r *recoveryController) Start() error {
	if r.eventStream == nil {
		return fmt.Errorf("the event stream is nil")
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

	klog.Info("the recoveryController is started")
	return nil
}

func (r *recoveryController) Stop() {
	close(r.stopCh)
}
