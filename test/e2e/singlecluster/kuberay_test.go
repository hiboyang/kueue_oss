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

package e2e

import (
	"fmt"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kueue/pkg/workloadslicing"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	workloadraycluster "sigs.k8s.io/kueue/pkg/controller/jobs/raycluster"
	workloadrayjob "sigs.k8s.io/kueue/pkg/controller/jobs/rayjob"
	"sigs.k8s.io/kueue/pkg/util/testing"
	testingraycluster "sigs.k8s.io/kueue/pkg/util/testingjobs/raycluster"
	testingrayjob "sigs.k8s.io/kueue/pkg/util/testingjobs/rayjob"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Kuberay", func() {
	const (
		resourceFlavorName = "kuberay-rf"
		clusterQueueName   = "kuberay-cq"
		localQueueName     = "kuberay-lq"
	)

	var (
		ns *corev1.Namespace
		rf *kueue.ResourceFlavor
		cq *kueue.ClusterQueue
		lq *kueue.LocalQueue
	)

	ginkgo.BeforeEach(func() {
		ns = util.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "kuberay-e2e-")
		rf = testing.MakeResourceFlavor(resourceFlavorName).
			NodeLabel("instance-type", "on-demand").
			Obj()
		gomega.Expect(k8sClient.Create(ctx, rf)).To(gomega.Succeed())

		cq = testing.MakeClusterQueue(clusterQueueName).
			ResourceGroup(
				*testing.MakeFlavorQuotas(rf.Name).
					Resource(corev1.ResourceCPU, "1").Obj()).
			Obj()
		gomega.Expect(k8sClient.Create(ctx, cq)).To(gomega.Succeed())

		lq = testing.MakeLocalQueue(localQueueName, ns.Name).ClusterQueue(cq.Name).Obj()
		gomega.Expect(k8sClient.Create(ctx, lq)).To(gomega.Succeed())
	})
	ginkgo.AfterEach(func() {
		gomega.Expect(util.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		util.ExpectObjectToBeDeleted(ctx, k8sClient, cq, true)
		util.ExpectObjectToBeDeleted(ctx, k8sClient, rf, true)
		util.ExpectAllPodsInNamespaceDeleted(ctx, k8sClient, ns)
	})

	ginkgo.It("Should run a rayjob if admitted", func() {
		kuberayTestImage := util.GetKuberayTestImage()

		rayJob := testingrayjob.MakeJob("rayjob", ns.Name).
			Queue(localQueueName).
			WithSubmissionMode(rayv1.K8sJobMode).
			Entrypoint("python -c \"import ray; ray.init(); print(ray.cluster_resources())\"").
			RequestAndLimit(rayv1.HeadNode, corev1.ResourceCPU, "300m").
			RequestAndLimit(rayv1.WorkerNode, corev1.ResourceCPU, "300m").
			WithSubmitterPodTemplate(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "rayjob-submitter",
							Image: kuberayTestImage,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("300m"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("300m"),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			}).
			Image(rayv1.HeadNode, kuberayTestImage).
			Image(rayv1.WorkerNode, kuberayTestImage).Obj()

		ginkgo.By("Creating the rayJob", func() {
			gomega.Expect(k8sClient.Create(ctx, rayJob)).Should(gomega.Succeed())
		})

		wlLookupKey := types.NamespacedName{Name: workloadrayjob.GetWorkloadNameForRayJob(rayJob.Name, rayJob.UID), Namespace: ns.Name}
		createdWorkload := &kueue.Workload{}
		ginkgo.By("Checking workload is created", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Checking workload is admitted", func() {
			util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, createdWorkload)
		})

		ginkgo.By("Waiting for the RayJob cluster become ready", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayJob := &rayv1.RayJob{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayJob), createdRayJob)).To(gomega.Succeed())
				g.Expect(createdRayJob.Spec.Suspend).To(gomega.BeFalse())
				g.Expect(createdRayJob.Status.JobDeploymentStatus).To(gomega.Equal(rayv1.JobDeploymentStatusRunning))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for the RayJob to finish", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayJob := &rayv1.RayJob{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayJob), createdRayJob)).To(gomega.Succeed())
				g.Expect(createdRayJob.Status.JobDeploymentStatus).To(gomega.Equal(rayv1.JobDeploymentStatusComplete))
				g.Expect(createdRayJob.Status.JobStatus).To(gomega.Equal(rayv1.JobStatusSucceeded))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})
	})

	ginkgo.It("Should run a rayjob with InTreeAutoscaling", func() {
		kuberayTestImage := util.GetKuberayTestImage()

		rayJob := testingrayjob.MakeJob("rayjob-autoscaling", ns.Name).
			Queue(localQueueName).
			AddAnnotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			WithSubmissionMode(rayv1.K8sJobMode).
			Entrypoint("python -c \"import ray; ray.init(); print(ray.cluster_resources())\"").
			RequestAndLimit(rayv1.HeadNode, corev1.ResourceCPU, "300m").
			RequestAndLimit(rayv1.WorkerNode, corev1.ResourceCPU, "300m").
			WithSubmitterPodTemplate(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "rayjob-submitter",
							Image: kuberayTestImage,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("300m"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("300m"),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			}).
			Image(rayv1.HeadNode, kuberayTestImage).
			Image(rayv1.WorkerNode, kuberayTestImage).Obj()

		ginkgo.By("Creating the rayJob", func() {
			gomega.Expect(k8sClient.Create(ctx, rayJob)).Should(gomega.Succeed())
		})

		ginkgo.By("Sleeping 60 seconds before listing resources", func() {
			fmt.Println("DEBUG: Sleeping for 60 seconds to allow workload creation...")
			time.Sleep(60 * time.Second)
			fmt.Println("DEBUG: Sleep complete, now listing resources")
		})

		ginkgo.By("DEBUG: Listing all RayJobs and Workloads", func() {
			// List all RayJobs in the namespace
			rayJobList := &rayv1.RayJobList{}
			gomega.Expect(k8sClient.List(ctx, rayJobList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			fmt.Printf("DEBUG: Found %d RayJobs in namespace %s:\n", len(rayJobList.Items), ns.Name)
			for i, rj := range rayJobList.Items {
				fmt.Printf("  [%d] Name: %s, UID: %s, Annotations: %v\n", i, rj.Name, rj.UID, rj.Annotations)
			}

			// List all Workloads in the namespace
			workloadList := &kueue.WorkloadList{}
			gomega.Expect(k8sClient.List(ctx, workloadList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			fmt.Printf("DEBUG: Found %d Workloads in namespace %s:\n", len(workloadList.Items), ns.Name)
			for i, wl := range workloadList.Items {
				fmt.Printf("  [%d] Name: %s, OwnerReferences: %v\n", i, wl.Name, wl.OwnerReferences)
			}

			// Print the expected workload name
			fmt.Printf("DEBUG: Expected workload name: %s\n", jobframework.GetWorkloadNameForOwnerWithGVK(rayJob.Name, rayJob.UID, rayv1.GroupVersion.WithKind("RayJob")))
			fmt.Printf("DEBUG: RayJob Name: %s, UID: %s\n", rayJob.Name, rayJob.UID)
		})

		ginkgo.By("DEBUG: Getting Kueue controller pod logs", func() {
			// Create a Kubernetes clientset
			clientset, err := kubernetes.NewForConfig(cfg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// List all pods in kueue-system namespace
			podList := &corev1.PodList{}
			kueueNamespace := "kueue-system"
			gomega.Expect(k8sClient.List(ctx, podList, client.InNamespace(kueueNamespace))).To(gomega.Succeed())
			fmt.Printf("DEBUG: Found %d pods in namespace %s:\n", len(podList.Items), kueueNamespace)

			// Print logs for each kueue controller pod
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
					fmt.Printf("\n=== DEBUG: Logs for pod %s in namespace %s ===\n", pod.Name, kueueNamespace)

					// Get logs for each container in the pod
					for _, container := range pod.Spec.Containers {
						fmt.Printf("\n--- Container: %s ---\n", container.Name)

						// Get pod logs
						logOptions := &corev1.PodLogOptions{
							Container: container.Name,
							TailLines: func(i int64) *int64 { return &i }(100), // Last 100 lines
						}

						req := clientset.CoreV1().Pods(kueueNamespace).GetLogs(pod.Name, logOptions)
						logs, err := req.DoRaw(ctx)
						if err != nil {
							fmt.Printf("Error getting logs for container %s: %v\n", container.Name, err)
							continue
						}
						fmt.Printf("%s\n", string(logs))
					}
					fmt.Printf("=== End logs for pod %s ===\n\n", pod.Name)
				}
			}
		})

		ginkgo.By("Checking at least one workload is created and admitted or finished", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				workloadList := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloadList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				g.Expect(workloadList.Items).NotTo(gomega.BeEmpty(), "Expected at least one workload in namespace")

				// Check that at least one workload is admitted
				hasAdmittedOrFinishedWorkload := false
				for _, wl := range workloadList.Items {
					if apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadAdmitted) || apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadFinished) {
						hasAdmittedOrFinishedWorkload = true
						break
					}
				}
				g.Expect(hasAdmittedOrFinishedWorkload).To(gomega.BeTrue(), "Expected at least one admitted or finished workload")
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("DEBUG: Listing all Ray job pods and their status", func() {
			// Create a Kubernetes clientset
			clientset, err := kubernetes.NewForConfig(cfg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// List all pods in all namespaces
			allPodList := &corev1.PodList{}
			gomega.Expect(k8sClient.List(ctx, allPodList)).To(gomega.Succeed())

			// Filter pods whose name contains 'ray'
			var rayPods []corev1.Pod
			for _, pod := range allPodList.Items {
				if strings.Contains(pod.Name, "ray") {
					rayPods = append(rayPods, pod)
				}
			}

			fmt.Printf("DEBUG: Found %d ray pods across all namespaces:\n", len(rayPods))
			for i, pod := range rayPods {
				fmt.Printf("  [%d] Namespace: %s, Name: %s, Phase: %s, Status: %+v\n",
					i, pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Conditions)
			}

			// Get logs for each ray pod
			for _, pod := range rayPods {
				if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
					fmt.Printf("\n=== DEBUG: Logs for ray pod %s in namespace %s ===\n", pod.Name, pod.Namespace)

					// Get logs for each container in the pod
					for _, container := range pod.Spec.Containers {
						fmt.Printf("\n--- Container: %s ---\n", container.Name)

						// Get pod logs
						logOptions := &corev1.PodLogOptions{
							Container: container.Name,
							TailLines: func(i int64) *int64 { return &i }(100), // Last 100 lines
						}

						req := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, logOptions)
						logs, err := req.DoRaw(ctx)
						if err != nil {
							fmt.Printf("Error getting logs for container %s: %v\n", container.Name, err)
							continue
						}
						fmt.Printf("%s\n", string(logs))
					}
					fmt.Printf("=== End logs for pod %s ===\n\n", pod.Name)
				}
			}
		})

		ginkgo.By("DEBUG: Getting Kuberay operator pod logs", func() {
			// Create a Kubernetes clientset
			clientset, err := kubernetes.NewForConfig(cfg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Try common kuberay operator namespaces
			kuberayNamespaces := []string{"ray-system", "kuberay-operator-system", "kuberay-system"}

			for _, kuberayNamespace := range kuberayNamespaces {
				// List all pods in kuberay namespace
				podList := &corev1.PodList{}
				err := k8sClient.List(ctx, podList, client.InNamespace(kuberayNamespace))
				if err != nil {
					fmt.Printf("DEBUG: Namespace %s not found or error: %v\n", kuberayNamespace, err)
					continue
				}

				if len(podList.Items) == 0 {
					fmt.Printf("DEBUG: No pods found in namespace %s\n", kuberayNamespace)
					continue
				}

				fmt.Printf("DEBUG: Found %d pods in namespace %s:\n", len(podList.Items), kuberayNamespace)

				// Print logs for each kuberay operator pod
				for _, pod := range podList.Items {
					if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded {
						fmt.Printf("\n=== DEBUG: Logs for pod %s in namespace %s ===\n", pod.Name, kuberayNamespace)

						// Get logs for each container in the pod
						for _, container := range pod.Spec.Containers {
							fmt.Printf("\n--- Container: %s ---\n", container.Name)

							// Get pod logs
							logOptions := &corev1.PodLogOptions{
								Container: container.Name,
								TailLines: func(i int64) *int64 { return &i }(100), // Last 100 lines
							}

							req := clientset.CoreV1().Pods(kuberayNamespace).GetLogs(pod.Name, logOptions)
							logs, err := req.DoRaw(ctx)
							if err != nil {
								fmt.Printf("Error getting logs for container %s: %v\n", container.Name, err)
								continue
							}
							fmt.Printf("%s\n", string(logs))
						}
						fmt.Printf("=== End logs for pod %s ===\n\n", pod.Name)
					}
				}
			}
		})

		ginkgo.By("Waiting for the RayJob cluster become ready", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayJob := &rayv1.RayJob{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayJob), createdRayJob)).To(gomega.Succeed())
				g.Expect(createdRayJob.Spec.Suspend).To(gomega.BeFalse())
				g.Expect(createdRayJob.Status.JobDeploymentStatus).To(gomega.Equal(rayv1.JobDeploymentStatusRunning))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for the RayJob to finish", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayJob := &rayv1.RayJob{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayJob), createdRayJob)).To(gomega.Succeed())
				g.Expect(createdRayJob.Status.JobDeploymentStatus).To(gomega.Equal(rayv1.JobDeploymentStatusComplete))
				g.Expect(createdRayJob.Status.JobStatus).To(gomega.Equal(rayv1.JobStatusSucceeded))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})
	})

	ginkgo.It("Should run a RayCluster on worker if admitted", func() {
		kuberayTestImage := util.GetKuberayTestImage()

		raycluster := testingraycluster.MakeCluster("raycluster1", ns.Name).
			Suspend(true).
			Queue(localQueueName).
			RequestAndLimit(rayv1.HeadNode, corev1.ResourceCPU, "300m").
			RequestAndLimit(rayv1.WorkerNode, corev1.ResourceCPU, "300m").
			Image(rayv1.HeadNode, kuberayTestImage, []string{}).
			Image(rayv1.WorkerNode, kuberayTestImage, []string{}).
			Obj()

		ginkgo.By("Creating the RayCluster", func() {
			gomega.Expect(k8sClient.Create(ctx, raycluster)).Should(gomega.Succeed())
		})

		wlLookupKey := types.NamespacedName{Name: workloadraycluster.GetWorkloadNameForRayCluster(raycluster.Name, raycluster.UID), Namespace: ns.Name}
		createdWorkload := &kueue.Workload{}
		ginkgo.By("Checking workload is created", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Checking workload is admitted", func() {
			util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, createdWorkload)
		})

		ginkgo.By("Checking the RayCluster is ready", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayCluster := &rayv1.RayCluster{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(raycluster), createdRayCluster)).To(gomega.Succeed())
				g.Expect(createdRayCluster.Status.DesiredWorkerReplicas).To(gomega.Equal(int32(1)))
				g.Expect(createdRayCluster.Status.ReadyWorkerReplicas).To(gomega.Equal(int32(1)))
				g.Expect(createdRayCluster.Status.AvailableWorkerReplicas).To(gomega.Equal(int32(1)))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})
	})
})
