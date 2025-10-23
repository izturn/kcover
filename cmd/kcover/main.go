package main

import (
	"context"
	"os"
	"time"

	"github.com/baizeai/kcover/pkg/diagnosis/controller"
	"github.com/baizeai/kcover/pkg/events"
	"github.com/baizeai/kcover/pkg/kube"
	"github.com/baizeai/kcover/pkg/recovery"
	"github.com/baizeai/kcover/pkg/runner"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

func mustHostName() string {
	hn, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hn
}
func lock(hostName string) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		Client: coordinationv1client.NewForConfigOrDie(kube.GetK8sConfigConfigWithFile("", "")),
		LeaseMeta: metav1.ObjectMeta{
			Name: "kcover",
			Namespace: func() string {
				if bs, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
					return string(bs)
				}
				return "default"
			}(),
		},
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: hostName,
		},
	}
}

func makeElectionCallback() (func(ctx context.Context), func()) {
	cfg := kube.GetK8sConfigConfigWithFile("", "")
	client := kubernetes.NewForConfigOrDie(cfg)

	var (
		eventBus events.Recorder
		recov    runner.Runner
		diag     runner.Runner
	)

	return func(context.Context) {
			// 当前实例成为 leader 时，开始执行 controller 逻辑
			var err error
			eventBus = events.NewKubeEventsRecorder(client, true)
			recov = recovery.NewRecoveryController(client, eventBus)
			diag, err = controller.NewDiagnostic(client, eventBus)
			if err != nil {
				panic(err)
			}
			if err := recov.Start(); err != nil {
				panic(err)
			}
			if err := diag.Start(); err != nil {
				panic(err)
			}
			if err := eventBus.Start(); err != nil {
				panic(err)
			}

			klog.Info("kcover is started")
		},
		func() {
			recov.Stop()
			diag.Stop()
			eventBus.Stop()
			klog.Info("kcover is stopped")
		}
}

func main() {
	started, stopped := makeElectionCallback()
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
