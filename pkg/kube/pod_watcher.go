package kube

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type PodHandlerFuncs struct {
	AddFunc    func(pod *corev1.Pod)
	UpdateFunc func(oldPod, newPod *corev1.Pod)
}

func WatchPods(cli kubernetes.Interface, stopCh <-chan struct{}, handlers PodHandlerFuncs) error {
	factory := informers.NewSharedInformerFactory(cli, time.Minute)
	informer := factory.Core().V1().Pods().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if handlers.AddFunc == nil {
				return
			}
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			handlers.AddFunc(pod)
		},
		UpdateFunc: func(oldObj, newObj any) {
			if handlers.UpdateFunc == nil {
				return
			}
			oldPod, ok := oldObj.(*corev1.Pod)
			if !ok {
				return
			}
			newPod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				return
			}
			handlers.UpdateFunc(oldPod, newPod)
		},
	})
	if err != nil {
		return err
	}

	go informer.Run(stopCh)
	return nil
}
