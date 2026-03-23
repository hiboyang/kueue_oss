/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package raycluster

import (
	"context"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
)

var _ handler.EventHandler = (*PodHandler)(nil)

// PodHandler maps Pod events to the grandparent owner of a RayCluster by walking the
// owner chain: Pod -> RayCluster (owner ref) -> parent (owner ref).
// The parentKind parameter specifies the expected Kind of the RayCluster's controller
// owner (e.g. "RayJob" or "RayService").
type PodHandler struct {
	Client     client.Client
	ParentKind string
}

func (h *PodHandler) Create(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.handle(e.Object, q)
}

func (h *PodHandler) Update(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.handle(e.ObjectNew, q)
}

func (h *PodHandler) Delete(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.handle(e.Object, q)
}

func (h *PodHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *PodHandler) handle(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	// Only process pods managed by Kueue.
	if pod.Annotations[kueue.WorkloadAnnotation] == "" {
		return
	}

	// Only process pods with workload slicing.
	if pod.Annotations[kueue.WorkloadSliceNameAnnotation] == "" {
		return
	}

	// Only process pods belonging to a RayCluster.
	if pod.Labels[rayutils.RayClusterLabelKey] == "" {
		return
	}

	// Find the RayCluster that owns this pod.
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil || controllerRef.Kind != "RayCluster" {
		return
	}

	// Fetch the RayCluster to find the parent owner.
	var cluster rayv1.RayCluster
	if err := h.Client.Get(context.Background(), types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      controllerRef.Name,
	}, &cluster); err != nil {
		ctrl.Log.V(3).Error(err, "Failed to get RayCluster for pod", "pod", pod.Name, "raycluster", controllerRef.Name)
		return
	}

	parentRef := metav1.GetControllerOf(&cluster)
	if parentRef == nil || parentRef.Kind != h.ParentKind {
		return
	}

	// Only process pods that have the elastic-job scheduling gate.
	if !utilpod.HasGate(pod, kueue.ElasticJobSchedulingGate) {
		return
	}

	q.AddAfter(reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      parentRef.Name,
		},
	}, constants.UpdatesBatchPeriod)
}
