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
	"github.com/google/go-cmp/cmp/cmpopts"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/kueue/pkg/features"
)

func TestGenerateWorkloadName(t *testing.T) {
	gvk := batchv1.SchemeGroupVersion.WithKind("Job")

	testCases := map[string]struct {
		featureGates map[featuregate.Feature]bool
		ownerName    string
		ownerUID     types.UID
		ownerGVK     schema.GroupVersionKind
		generation   *int64
		want         string
	}{
		"should generate workload name (ShortWorkloadNames=false)": {
			featureGates: map[featuregate.Feature]bool{
				features.ShortWorkloadNames: false,
			},
			ownerName:  "name",
			ownerUID:   "uid",
			ownerGVK:   gvk,
			generation: nil,
			want:       "job-name-ef3a1",
		},
		"should generate workload name (ShortWorkloadNames=false, with generation)": {
			featureGates: map[featuregate.Feature]bool{
				features.ShortWorkloadNames: false,
			},
			ownerName:  "name",
			ownerUID:   "uid",
			ownerGVK:   gvk,
			generation: ptr.To[int64](1),
			want:       "job-name-2514c",
		},
		"should generate workload name (ShortWorkloadNames=false, 260 symbols owner name)": {
			featureGates: map[featuregate.Feature]bool{
				features.ShortWorkloadNames: false,
			},
			ownerName:  "marketing-campaign-autumn-2024-region-north-america-department-digital-strategy-sub-department-social-media-engagement-tracker-and-analytics-dashboard-v2-final-revision-by-engineering-team-lead-senior-architect-validation-checked-approved-by-legal-and-ops-dept",
			ownerUID:   "uid",
			ownerGVK:   gvk,
			generation: nil,
			want:       "job-marketing-campaign-autumn-2024-region-north-america-department-digital-strategy-sub-department-social-media-engagement-tracker-and-analytics-dashboard-v2-final-revision-by-engineering-team-lead-senior-architect-validation-checked-approved-by-l-218b8",
		},
		"should generate workload name (ShortWorkloadNames=true, 260 symbols owner name)": {
			featureGates: map[featuregate.Feature]bool{
				features.ShortWorkloadNames: true,
			},
			ownerName:  "marketing-campaign-autumn-2024-region-north-america-department-digital-strategy-sub-department-social-media-engagement-tracker-and-analytics-dashboard-v2-final-revision-by-engineering-team-lead-senior-architect-validation-checked-approved-by-legal-and-ops-dept",
			ownerUID:   "uid",
			ownerGVK:   gvk,
			generation: nil,
			want:       "job-marketing-campaign-autumn-2024-region-north-america-d-218b8",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for fg, enabled := range tc.featureGates {
				features.SetFeatureGateDuringTest(t, fg, enabled)
			}
			got := generateWorkloadName(tc.ownerName, tc.ownerUID, tc.ownerGVK, tc.generation)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateErrors()); len(diff) != 0 {
				t.Fatalf("Unexpected workloadName (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestGenerateWorkloadNameWithExtra(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}

	cases := map[string]struct {
		ownerName  string
		ownerUID   types.UID
		ownerGVK   schema.GroupVersionKind
		extra      string
		wantPrefix string
	}{
		"empty extra": {
			ownerName:  "my-job",
			ownerUID:   "uid-123",
			ownerGVK:   gvk,
			extra:      "",
			wantPrefix: "job-my-job-",
		},
		"generation as extra": {
			ownerName:  "my-job",
			ownerUID:   "uid-123",
			ownerGVK:   gvk,
			extra:      "1",
			wantPrefix: "job-my-job-",
		},
		"resourceVersion as extra": {
			ownerName:  "my-job",
			ownerUID:   "uid-123",
			ownerGVK:   gvk,
			extra:      "100",
			wantPrefix: "job-my-job-",
		},
		"custom extra string": {
			ownerName:  "my-job",
			ownerUID:   "uid-123",
			ownerGVK:   gvk,
			extra:      "custom-part",
			wantPrefix: "job-my-job-",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := GenerateWorkloadNameWithExtra(tc.ownerName, tc.ownerUID, tc.ownerGVK, tc.extra)
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("GenerateWorkloadNameWithExtra() = %q, want prefix %q", got, tc.wantPrefix)
			}
			// Name should have prefix + "-" + 5-char hash
			prefix := GenerateWorkloadNamePrefix(tc.ownerName, tc.ownerUID, tc.ownerGVK)
			if len(got) != len(prefix)+1+hashLength {
				t.Errorf("GenerateWorkloadNameWithExtra() length = %d, want %d (prefix=%d + 1 + hash=%d)", len(got), len(prefix)+1+hashLength, len(prefix), hashLength)
			}
		})
	}

	// Verify that different extra values produce different names.
	t.Run("different extra values produce different names", func(t *testing.T) {
		name1 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "1")
		name2 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "2")
		if name1 == name2 {
			t.Errorf("expected different names for different extra values, got %q for both", name1)
		}
	})

	t.Run("different resourceVersion values produce different names", func(t *testing.T) {
		name1 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "100")
		name2 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "200")
		if name1 == name2 {
			t.Errorf("expected different names for different extra values, got %q for both", name1)
		}
	})

	t.Run("same extra produces same name", func(t *testing.T) {
		name1 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "100")
		name2 := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "100")
		if diff := cmp.Diff(name1, name2); diff != "" {
			t.Errorf("expected same name for same extra value (-want,+got):\n%s", diff)
		}
	})

	t.Run("same prefix as GetWorkloadNameForOwnerWithGVK", func(t *testing.T) {
		elasticName := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "1")
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

func TestGenerateWorkloadNameWithGeneration(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}

	t.Run("different generations produce different names", func(t *testing.T) {
		name1 := GetWorkloadNameForOwnerWithGVKAndGeneration("my-job", "uid-123", gvk, 1)
		name2 := GetWorkloadNameForOwnerWithGVKAndGeneration("my-job", "uid-123", gvk, 2)
		if name1 == name2 {
			t.Errorf("expected different names for different generations, got %q for both", name1)
		}
	})

	t.Run("same generation produces same name", func(t *testing.T) {
		name1 := GetWorkloadNameForOwnerWithGVKAndGeneration("my-job", "uid-123", gvk, 1)
		name2 := GetWorkloadNameForOwnerWithGVKAndGeneration("my-job", "uid-123", gvk, 1)
		if diff := cmp.Diff(name1, name2); diff != "" {
			t.Errorf("expected same name for same generation (-want,+got):\n%s", diff)
		}
	})

	t.Run("consistent with GenerateWorkloadNameWithExtra using string generation", func(t *testing.T) {
		nameFromGeneration := GetWorkloadNameForOwnerWithGVKAndGeneration("my-job", "uid-123", gvk, 42)
		nameFromExtra := GenerateWorkloadNameWithExtra("my-job", "uid-123", gvk, "42")
		if diff := cmp.Diff(nameFromGeneration, nameFromExtra); diff != "" {
			t.Errorf("GenerateWorkloadNameWithGeneration and GenerateWorkloadNameWithExtra should produce same result (-want,+got):\n%s", diff)
		}
	})
}
