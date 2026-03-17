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
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const (
	hashLength = 5
	// 253 is the maximal length for a CRD name. We need to subtract one for '-', and the hash length.
	maxPrefixLength = 252 - hashLength
)

func GetWorkloadNameForOwnerWithGVK(ownerName string, ownerUID types.UID, ownerGVK schema.GroupVersionKind) string {
	return generateWorkloadNameWithExtra(ownerName, ownerUID, ownerGVK, "")
}

// ElasticWorkloadNameProvider interface contains methods to provide extra information to build workload name for elastic job
type ElasticWorkloadNameProvider interface {
	GetGeneration() int64
	GetAnnotations() map[string]string
}

func GetElasticWorkloadName(ownerName string, ownerUID types.UID, ownerGVK schema.GroupVersionKind, elasticWorkloadNameProvider ElasticWorkloadNameProvider) string {
	extra := elasticWorkloadNameProvider.GetAnnotations()[PodsetReplicaSizesAnnotation]
	if extra == "" {
		extra = strconv.FormatInt(elasticWorkloadNameProvider.GetGeneration(), 10)
	}
	return generateWorkloadNameWithExtra(ownerName, ownerUID, ownerGVK, extra)
}

func GenerateWorkloadNamePrefix(ownerName string, ownerUID types.UID, ownerGVK schema.GroupVersionKind) string {
	prefixedName := strings.ToLower(ownerGVK.Kind) + "-" + ownerName
	if len(prefixedName) > maxPrefixLength {
		prefixedName = prefixedName[:maxPrefixLength]
	}
	return prefixedName
}

func generateWorkloadNameWithExtra(ownerName string, ownerUID types.UID, ownerGVK schema.GroupVersionKind, extra string) string {
	prefixedName := GenerateWorkloadNamePrefix(ownerName, ownerUID, ownerGVK)
	return prefixedName + "-" + getHash(ownerName, ownerUID, ownerGVK, extra)[:hashLength]
}

func getHash(ownerName string, ownerUID types.UID, gvk schema.GroupVersionKind, extra string) string {
	h := sha1.New()
	h.Write([]byte(gvk.Kind))
	h.Write([]byte("\n"))
	h.Write([]byte(gvk.Group))
	h.Write([]byte("\n"))
	h.Write([]byte(ownerName))
	h.Write([]byte("\n"))
	h.Write([]byte(ownerUID))
	if extra != "" {
		h.Write([]byte("\n"))
		h.Write([]byte(extra))
	}
	return hex.EncodeToString(h.Sum(nil))
}
