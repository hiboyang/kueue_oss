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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
)

func TestPodHandler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	rayJobUID := types.UID("ray-job-uid")
	rayClusterUID := types.UID("ray-cluster-uid")

	makeRayCluster := func(name, ns string, ownerKind string, ownerName string, ownerUID types.UID) *rayv1.RayCluster {
		cluster := &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				UID:       rayClusterUID,
			},
		}
		if ownerKind != "" {
			cluster.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: rayv1.GroupVersion.String(),
					Kind:       ownerKind,
					Name:       ownerName,
					UID:        ownerUID,
					Controller: boolPtr(true),
				},
			}
		}
		return cluster
	}

	makePod := func(name, ns string, labels map[string]string, annotations map[string]string, gates []corev1.PodSchedulingGate, ownerKind, ownerName string, ownerUID types.UID) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   ns,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				SchedulingGates: gates,
			},
		}
		if ownerKind != "" {
			pod.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: rayv1.GroupVersion.String(),
					Kind:       ownerKind,
					Name:       ownerName,
					UID:        ownerUID,
					Controller: boolPtr(true),
				},
			}
		}
		return pod
	}

	elasticGate := []corev1.PodSchedulingGate{{Name: kueue.ElasticJobSchedulingGate}}

	testCases := map[string]struct {
		pod            *corev1.Pod
		clusterInStore *rayv1.RayCluster
		parentKind     string
		wantRequests   []reconcile.Request
	}{
		"pod with matching annotations, labels, gate, and owner chain enqueues request": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "my-rayjob"}},
			},
		},
		"pod without workload annotation is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, nil, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"pod without ray cluster label is skipped": {
			pod: makePod("pod1", "ns", nil, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"pod without owner reference is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "", "", ""),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"pod owned by non-RayCluster kind is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "StatefulSet", "my-sts", types.UID("sts-uid")),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"RayCluster not found in store is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "nonexistent-cluster", rayClusterUID),
			clusterInStore: nil,
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"RayCluster without owner reference is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "", "", ""),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"RayCluster owned by different kind than parentKind is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayService", "my-service", types.UID("svc-uid")),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"RayService parentKind enqueues correct request": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayService", "my-rayservice", types.UID("svc-uid")),
			parentKind:     "RayService",
			wantRequests: []reconcile.Request{
				{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "my-rayservice"}},
			},
		},
		"non-pod object is skipped": {
			pod:            nil, // will pass a non-pod object
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
		"pod without elastic-job scheduling gate is skipped": {
			pod: makePod("pod1", "ns", map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, nil, "RayCluster", "my-cluster", rayClusterUID),
			clusterInStore: makeRayCluster("my-cluster", "ns", "RayJob", "my-rayjob", rayJobUID),
			parentKind:     "RayJob",
			wantRequests:   nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objs []client.Object
			if tc.clusterInStore != nil {
				objs = append(objs, tc.clusterInStore)
			}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			h := &PodHandler{
				Client:     c,
				ParentKind: tc.parentKind,
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			if tc.pod == nil {
				// Test with a non-pod object.
				h.handle(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "ns"}}, q)
			} else {
				h.handle(tc.pod, q)
			}

			var gotRequests []reconcile.Request
			// Items are enqueued via AddAfter with constants.UpdatesBatchPeriod (1s).
			waitDuration := constants.UpdatesBatchPeriod + 500*time.Millisecond
			if len(tc.wantRequests) > 0 {
				for range len(tc.wantRequests) {
					// q.Get() blocks until the delayed item becomes available.
					item, shutdown := q.Get()
					if shutdown {
						t.Fatal("Queue shut down unexpectedly")
					}
					gotRequests = append(gotRequests, item)
					q.Done(item)
				}
			} else {
				// Confirm nothing was enqueued after the batch period elapses.
				time.Sleep(waitDuration)
				if q.Len() > 0 {
					item, _ := q.Get()
					t.Errorf("Expected no requests but got: %v", item)
				}
			}

			if diff := cmp.Diff(tc.wantRequests, gotRequests); diff != "" {
				t.Errorf("Unexpected requests (-want +got):\n%s", diff)
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
