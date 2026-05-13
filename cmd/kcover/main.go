package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/baizeai/kcover/pkg/detector/pod"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"
	"github.com/baizeai/kcover/pkg/preflight"
	"github.com/baizeai/kcover/pkg/recovery"
	"github.com/baizeai/kcover/pkg/runner"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

var preflightReportCollectionTimeout = flag.Duration(
	"preflight-report-collection-timeout",
	preflight.DefaultReportCollectionTimeout,
	"maximum time to wait for a complete set of preflight reports before expiring the partial aggregation",
)

func mustHostName() string {
	if nodeName := kube.NodeNameFromEnv(); nodeName != "" {
		return nodeName
	}

	hn, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hn
}
func lock(hostName string) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		Client: coordv1.NewForConfigOrDie(kube.GetK8sConfigConfigWithFile("", "")),
		LeaseMeta: metav1.ObjectMeta{
			Name:      "kcover",
			Namespace: kube.CurrentNamespace(),
		},
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: hostName,
		},
	}
}

func makeElectionCallback(reportCollectionTimeout time.Duration) (func(ctx context.Context), func()) {
	cfg := kube.GetK8sConfigConfigWithFile("", "")
	client := kubernetes.NewForConfigOrDie(cfg)

	var (
		recov    runner.Runner
		detector runner.Runner
		bridge   events.Bridge
	)

	return func(context.Context) {
			// 当前实例成为 leader 时，开始执行 controller 逻辑
			var err error
			bridge = events.NewKubeEventBridge(client)
			recov = recovery.NewController(client, bridge, recovery.ControllerOptions{
				PreflightReportCollectionTimeout: reportCollectionTimeout,
			})
			detector, err = pod.NewDetector(client, bridge)
			if err != nil {
				panic(err)
			}
			if err := recov.Start(); err != nil {
				panic(err)
			}
			if err := detector.Start(); err != nil {
				panic(err)
			}
			if err := bridge.Start(); err != nil {
				panic(err)
			}

			klog.Info("kcover is started")
		},
		func() {
			recov.Stop()
			detector.Stop()
			bridge.Stop()
			klog.Info("kcover is stopped")
		}
}

func main() {
	flag.Parse()
	started, stopped := makeElectionCallback(*preflightReportCollectionTimeout)
	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock(mustHostName()),
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: started,
			OnStoppedLeading: stopped,
		},
	}
	leaderelection.RunOrDie(context.Background(), leaderElectionConfig)
}
