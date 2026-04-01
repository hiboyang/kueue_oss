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

	"github.com/google/go-cmp/cmp"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
)

func TestShouldTriggerReconcileForRayClusterOwner(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	rayClusterUID := types.UID("ray-cluster-uid")
	elasticGate := []corev1.PodSchedulingGate{{Name: kueue.ElasticJobSchedulingGate}}

	makePod := func(labels map[string]string, annotations map[string]string, gates []corev1.PodSchedulingGate, ownerKind, ownerName string, ownerUID types.UID) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pod1",
				Namespace:   "ns",
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

	testCases := map[string]struct {
		pod             *corev1.Pod
		wantClusterName *string
	}{
		"pod with all conditions met returns RayCluster name": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation:          "my-workload",
				kueue.WorkloadSliceNameAnnotation: "my-workload-slice",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			wantClusterName: strPtr("my-cluster"),
		},
		"pod without workload annotation returns nil": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, nil, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			wantClusterName: nil,
		},
		"pod without workload slice annotation returns nil": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation: "my-workload",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			wantClusterName: nil,
		},
		"pod without ray cluster label returns nil": {
			pod: makePod(nil, map[string]string{
				kueue.WorkloadAnnotation:          "my-workload",
				kueue.WorkloadSliceNameAnnotation: "my-workload-slice",
			}, elasticGate, "RayCluster", "my-cluster", rayClusterUID),
			wantClusterName: nil,
		},
		"pod without elastic-job scheduling gate returns nil": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation:          "my-workload",
				kueue.WorkloadSliceNameAnnotation: "my-workload-slice",
			}, nil, "RayCluster", "my-cluster", rayClusterUID),
			wantClusterName: nil,
		},
		"pod without owner reference returns nil": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation:          "my-workload",
				kueue.WorkloadSliceNameAnnotation: "my-workload-slice",
			}, elasticGate, "", "", ""),
			wantClusterName: nil,
		},
		"pod owned by non-RayCluster kind returns nil": {
			pod: makePod(map[string]string{
				rayutils.RayClusterLabelKey: "my-cluster",
			}, map[string]string{
				kueue.WorkloadAnnotation:          "my-workload",
				kueue.WorkloadSliceNameAnnotation: "my-workload-slice",
			}, elasticGate, "StatefulSet", "my-sts", types.UID("sts-uid")),
			wantClusterName: nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got := ShouldTriggerReconcileForRayClusterOwner(tc.pod)
			if diff := cmp.Diff(tc.wantClusterName, got); diff != "" {
				t.Errorf("Unexpected result (-want +got):\n%s", diff)
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func strPtr(s string) *string {
	return &s
}
