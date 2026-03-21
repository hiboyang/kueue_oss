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

package jobframework

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func TestGetElasticWorkloadName(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}

	cases := map[string]struct {
		ownerName  string
		ownerUID   types.UID
		ownerGVK   schema.GroupVersionKind
		provider   ElasticWorkloadNameProvider
		wantPrefix string
	}{
		"uses generation when no annotation": {
			ownerName: "my-job",
			ownerUID:  "uid-123",
			ownerGVK:  gvk,
			provider: &fakeElasticProvider{
				generation:  1,
				annotations: nil,
			},
			wantPrefix: "job-my-job-",
		},
		"uses resourceVersion when annotation present": {
			ownerName: "my-job",
			ownerUID:  "uid-123",
			ownerGVK:  gvk,
			provider: &fakeElasticProvider{
				generation:      1,
				resourceVersion: "100",
				annotations: map[string]string{
					PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`,
				},
			},
			wantPrefix: "job-my-job-",
		},
		"different generation produces different name": {
			ownerName: "my-job",
			ownerUID:  "uid-123",
			ownerGVK:  gvk,
			provider: &fakeElasticProvider{
				generation:  2,
				annotations: nil,
			},
			wantPrefix: "job-my-job-",
		},
		"different resourceVersion produces different name": {
			ownerName: "my-job",
			ownerUID:  "uid-123",
			ownerGVK:  gvk,
			provider: &fakeElasticProvider{
				generation:      1,
				resourceVersion: "200",
				annotations: map[string]string{
					PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`,
				},
			},
			wantPrefix: "job-my-job-",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := GetElasticWorkloadName(tc.ownerName, tc.ownerUID, tc.ownerGVK, tc.provider)
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("GetElasticWorkloadName() = %q, want prefix %q", got, tc.wantPrefix)
			}
			// Name should have prefix + "-" + 5-char hash
			prefix := GenerateWorkloadNamePrefix(tc.ownerName, tc.ownerUID, tc.ownerGVK)
			if len(got) != len(prefix)+1+hashLength {
				t.Errorf("GetElasticWorkloadName() length = %d, want %d (prefix=%d + 1 + hash=%d)", len(got), len(prefix)+1+hashLength, len(prefix), hashLength)
			}
		})
	}

	// Verify that different extra values produce different names.
	t.Run("different generation values produce different names", func(t *testing.T) {
		name1 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{generation: 1})
		name2 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{generation: 2})
		if name1 == name2 {
			t.Errorf("expected different names for different generations, got %q for both", name1)
		}
	})

	t.Run("different resourceVersion values produce different names", func(t *testing.T) {
		name1 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      1,
			resourceVersion: "100",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`},
		})
		name2 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      1,
			resourceVersion: "200",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`},
		})
		if name1 == name2 {
			t.Errorf("expected different names for different resourceVersions, got %q for both", name1)
		}
	})

	t.Run("same resourceVersion same name regardless of annotation content", func(t *testing.T) {
		name1 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      1,
			resourceVersion: "100",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`},
		})
		name2 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      1,
			resourceVersion: "100",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":5}]`},
		})
		if diff := cmp.Diff(name1, name2); diff != "" {
			t.Errorf("expected same name for same resourceVersion regardless of annotation content (-want,+got):\n%s", diff)
		}
	})

	t.Run("resourceVersion takes priority over generation", func(t *testing.T) {
		// Same resourceVersion but different generation should produce the same name.
		name1 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      1,
			resourceVersion: "100",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`},
		})
		name2 := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{
			generation:      99,
			resourceVersion: "100",
			annotations:     map[string]string{PodsetReplicaSizesAnnotation: `[{"name":"main","count":3}]`},
		})
		if diff := cmp.Diff(name1, name2); diff != "" {
			t.Errorf("expected same name when resourceVersion matches regardless of generation (-want,+got):\n%s", diff)
		}
	})

	t.Run("same prefix as GetWorkloadNameForOwnerWithGVK", func(t *testing.T) {
		elasticName := GetElasticWorkloadName("my-job", "uid-123", gvk, &fakeElasticProvider{generation: 1})
		staticName := GetWorkloadNameForOwnerWithGVK("my-job", "uid-123", gvk)
		prefix := GenerateWorkloadNamePrefix("my-job", "uid-123", gvk)
		if !strings.HasPrefix(elasticName, prefix) {
			t.Errorf("elastic name %q does not have expected prefix %q", elasticName, prefix)
		}
		if !strings.HasPrefix(staticName, prefix) {
			t.Errorf("static name %q does not have expected prefix %q", staticName, prefix)
		}
		// The hash suffix should differ since elastic uses extra.
		if elasticName == staticName {
			t.Errorf("elastic and static names should differ, both are %q", elasticName)
		}
	})
}

type fakeElasticProvider struct {
	generation      int64
	resourceVersion string
	annotations     map[string]string
}

func (f *fakeElasticProvider) GetGeneration() int64 {
	return f.generation
}

func (f *fakeElasticProvider) GetResourceVersion() string {
	return f.resourceVersion
}

func (f *fakeElasticProvider) GetAnnotations() map[string]string {
	return f.annotations
}
