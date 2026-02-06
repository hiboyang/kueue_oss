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
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	workloadraycluster "sigs.k8s.io/kueue/pkg/controller/jobs/raycluster"
	workloadrayjob "sigs.k8s.io/kueue/pkg/controller/jobs/rayjob"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingraycluster "sigs.k8s.io/kueue/pkg/util/testingjobs/raycluster"
	testingrayjob "sigs.k8s.io/kueue/pkg/util/testingjobs/rayjob"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Kuberay", func() {
	var (
		ns                 *corev1.Namespace
		rf                 *kueue.ResourceFlavor
		cq                 *kueue.ClusterQueue
		lq                 *kueue.LocalQueue
		resourceFlavorName string
		clusterQueueName   string
		localQueueName     string
	)

	// getRunningWorkerPodNames returns the names of running pods that have "workers" in their name
	getRunningWorkerPodNames := func(podList *corev1.PodList) []string {
		var podNames []string
		for _, pod := range podList.Items {
			if strings.Contains(pod.Name, "workers") && pod.Status.Phase == corev1.PodRunning {
				podNames = append(podNames, pod.Name)
			}
		}
		return podNames
	}

	ginkgo.BeforeEach(func() {
		ns = util.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "kuberay-e2e-")
		resourceFlavorName = "kuberay-rf-" + ns.Name
		clusterQueueName = "kuberay-cq-" + ns.Name
		localQueueName = "kuberay-lq-" + ns.Name

		rf = utiltestingapi.MakeResourceFlavor(resourceFlavorName).
			NodeLabel("instance-type", "on-demand").
			Obj()
		gomega.Expect(k8sClient.Create(ctx, rf)).To(gomega.Succeed())

		cq = utiltestingapi.MakeClusterQueue(clusterQueueName).
			ResourceGroup(
				*utiltestingapi.MakeFlavorQuotas(rf.Name).
					Resource(corev1.ResourceCPU, "3").Obj()).
			Obj()
		util.CreateClusterQueuesAndWaitForActive(ctx, k8sClient, cq)

		lq = utiltestingapi.MakeLocalQueue(localQueueName, ns.Name).ClusterQueue(cq.Name).Obj()
		util.CreateLocalQueuesAndWaitForActive(ctx, k8sClient, lq)
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

		// Create kubernetes clientset for pod logs
		clientset, err := kubernetes.NewForConfig(cfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Update kueue-controller-manager deployment to change log level from 2 to 12
		ginkgo.By("Updating kueue-controller-manager deployment log level to 12", func() {
			deployKey := types.NamespacedName{
				Name:      "kueue-controller-manager",
				Namespace: "kueue-system",
			}
			deploy := &appsv1.Deployment{}
			gomega.Expect(k8sClient.Get(ctx, deployKey, deploy)).To(gomega.Succeed())

			// Find and update the --zap-log-level argument
			for i := range deploy.Spec.Template.Spec.Containers {
				container := &deploy.Spec.Template.Spec.Containers[i]
				for j, arg := range container.Args {
					if strings.HasPrefix(arg, "--zap-log-level=") {
						container.Args[j] = "--zap-log-level=12"
					}
				}
			}

			gomega.Expect(k8sClient.Update(ctx, deploy)).To(gomega.Succeed())
		})

		// Wait for the deployment to be ready after the update
		ginkgo.By("Waiting for kueue-controller-manager deployment to be ready after log level update", func() {
			deployKey := types.NamespacedName{
				Name:      "kueue-controller-manager",
				Namespace: "kueue-system",
			}
			gomega.Eventually(func(g gomega.Gomega) {
				deploy := &appsv1.Deployment{}
				g.Expect(k8sClient.Get(ctx, deployKey, deploy)).To(gomega.Succeed())
				g.Expect(deploy.Status.ReadyReplicas).To(gomega.Equal(*deploy.Spec.Replicas))
				g.Expect(deploy.Status.AvailableReplicas).To(gomega.Equal(*deploy.Spec.Replicas))
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		// Create ConfigMap with Python script
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayjob-autoscaling",
				Namespace: ns.Name,
			},
			Data: map[string]string{
				"sample_code.py": `import ray
import os

ray.init()

@ray.remote
def my_task(x, s):
    import time
    time.sleep(s)
    return x * x

# run tasks in sequence to avoid triggering autoscaling in the beginning
print([ray.get(my_task.remote(i, 1)) for i in range(4)])

# run tasks in parallel to trigger autoscaling (scaling up)
print(ray.get([my_task.remote(i, 4) for i in range(16)]))

# run tasks in sequence to trigger scaling down
print([ray.get(my_task.remote(i, 1)) for i in range(16)])`,
			},
		}

		rayJob := testingrayjob.MakeJob("rayjob-autoscaling", ns.Name).
			Queue(localQueueName).
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			EnableInTreeAutoscaling().
			WithSubmissionMode(rayv1.K8sJobMode).
			Entrypoint("python /home/ray/samples/sample_code.py").
			RequestAndLimit(rayv1.HeadNode, corev1.ResourceCPU, "200m").
			RequestAndLimit(rayv1.WorkerNode, corev1.ResourceCPU, "200m").
			WithSubmitterPodTemplate(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "rayjob-submitter",
							Image: kuberayTestImage,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("200m"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse("200m"),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			}).
			Image(rayv1.HeadNode, kuberayTestImage).
			Image(rayv1.WorkerNode, kuberayTestImage).Obj()

		// Add volume and volumeMount to head node for the ConfigMap
		rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "script-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "rayjob-autoscaling",
						},
					},
				},
			},
		}
		rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "script-volume",
				MountPath: "/home/ray/samples",
			},
		}
		rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(5))
		for i := range len(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs) {
			rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[i].Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(5))
		}

		ginkgo.By("Creating the ConfigMap", func() {
			gomega.Expect(k8sClient.Create(ctx, configMap)).Should(gomega.Succeed())
		})

		ginkgo.By("Creating the rayJob", func() {
			gomega.Expect(k8sClient.Create(ctx, rayJob)).Should(gomega.Succeed())
		})

		// Variable to store initial pod names for verification during scaling
		var initialPodNames []string
		// Variable to store scaled-up pod names for verification during scaling down
		var scaledUpPodNames []string

		ginkgo.By("Checking one workload is created", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				workloadList := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloadList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				g.Expect(workloadList.Items).NotTo(gomega.BeEmpty(), "Expected at least one workload in namespace")
				hasAdmittedWorkload := false
				for _, wl := range workloadList.Items {
					if apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadAdmitted) ||
						apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadFinished) {
						hasAdmittedWorkload = true
						break
					}
				}
				g.Expect(hasAdmittedWorkload).To(gomega.BeTrue(), "Expected admitted workload")
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		// Wait 60 seconds and print debug information before checking RayJob cluster ready
		ginkgo.By("Waiting 60 seconds and printing debug information", func() {
			fmt.Println("Waiting 60 seconds before printing debug information...")
			time.Sleep(60 * time.Second)

			// Print all ClusterQueues with full YAML
			fmt.Println("=== All ClusterQueues ===")
			cqList := &kueue.ClusterQueueList{}
			if err := k8sClient.List(ctx, cqList); err != nil {
				fmt.Printf("Failed to list ClusterQueues: %v\n", err)
			} else {
				for _, item := range cqList.Items {
					yamlBytes, err := yaml.Marshal(item)
					if err != nil {
						fmt.Printf("Failed to marshal ClusterQueue %s: %v\n", item.Name, err)
					} else {
						fmt.Printf("ClusterQueue %s:\n%s\n", item.Name, string(yamlBytes))
					}
				}
			}
			fmt.Println("=== End of ClusterQueues ===")

			// Print all Workloads with full YAML
			fmt.Println("=== All Workloads ===")
			wlList := &kueue.WorkloadList{}
			if err := k8sClient.List(ctx, wlList); err != nil {
				fmt.Printf("Failed to list Workloads: %v\n", err)
			} else {
				for _, item := range wlList.Items {
					yamlBytes, err := yaml.Marshal(item)
					if err != nil {
						fmt.Printf("Failed to marshal Workload %s/%s: %v\n", item.Namespace, item.Name, err)
					} else {
						fmt.Printf("Workload %s/%s:\n%s\n", item.Namespace, item.Name, string(yamlBytes))
					}
				}
			}
			fmt.Println("=== End of Workloads ===")

			// Print all RayJobs with full YAML
			fmt.Println("=== All RayJobs ===")
			rayJobList := &rayv1.RayJobList{}
			if err := k8sClient.List(ctx, rayJobList); err != nil {
				fmt.Printf("Failed to list RayJobs: %v\n", err)
			} else {
				for _, item := range rayJobList.Items {
					yamlBytes, err := yaml.Marshal(item)
					if err != nil {
						fmt.Printf("Failed to marshal RayJob %s/%s: %v\n", item.Namespace, item.Name, err)
					} else {
						fmt.Printf("RayJob %s/%s:\n%s\n", item.Namespace, item.Name, string(yamlBytes))
					}
				}
			}
			fmt.Println("=== End of RayJobs ===")

			// Print all batch/Jobs with full YAML
			fmt.Println("=== All batch/Jobs ===")
			jobList := &batchv1.JobList{}
			if err := k8sClient.List(ctx, jobList); err != nil {
				fmt.Printf("Failed to list Jobs: %v\n", err)
			} else {
				for _, item := range jobList.Items {
					yamlBytes, err := yaml.Marshal(item)
					if err != nil {
						fmt.Printf("Failed to marshal Job %s/%s: %v\n", item.Namespace, item.Name, err)
					} else {
						fmt.Printf("Job %s/%s:\n%s\n", item.Namespace, item.Name, string(yamlBytes))
					}
				}
			}
			fmt.Println("=== End of batch/Jobs ===")

			// List all pods and their status
			fmt.Println("=== All Pods Status ===")
			allPodList := &corev1.PodList{}
			if err := k8sClient.List(ctx, allPodList); err != nil {
				fmt.Printf("Failed to list all pods: %v\n", err)
			} else {
				for _, pod := range allPodList.Items {
					fmt.Printf("Pod %s/%s: Phase=%s, Conditions=%v\n",
						pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Conditions)
				}
			}
			fmt.Println("=== End of All Pods Status ===")

			// Print logs for pods with "ray" in name
			fmt.Println("=== Logs for Ray Pods ===")
			for _, pod := range allPodList.Items {
				if strings.Contains(strings.ToLower(pod.Name), "ray") {
					for _, container := range pod.Spec.Containers {
						fmt.Printf("=== Logs for ray pod %s/%s, container %s ===\n", pod.Namespace, pod.Name, container.Name)

						req := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
							Container: container.Name,
						})
						logStream, err := req.Stream(context.Background())
						if err != nil {
							fmt.Printf("Failed to get logs for pod %s, container %s: %v\n", pod.Name, container.Name, err)
							continue
						}

						buf := new(bytes.Buffer)
						_, err = io.Copy(buf, logStream)
						logStream.Close()
						if err != nil {
							fmt.Printf("Failed to read logs for pod %s, container %s: %v\n", pod.Name, container.Name, err)
							continue
						}

						fmt.Println(buf.String())
						fmt.Printf("=== End of logs for ray pod %s/%s, container %s ===\n", pod.Namespace, pod.Name, container.Name)
					}
				}
			}
			fmt.Println("=== End of Logs for Ray Pods ===")

			// Print logs for kueue controller pods
			fmt.Println("=== Logs for Kueue Controller Pods ===")
			kueuePodList := &corev1.PodList{}
			gomega.Expect(k8sClient.List(ctx, kueuePodList, client.InNamespace("kueue-system"))).To(gomega.Succeed())

			for _, pod := range kueuePodList.Items {
				if strings.Contains(pod.Name, "kueue-controller-manager") {
					for _, container := range pod.Spec.Containers {
						fmt.Printf("=== Logs for pod %s, container %s ===\n", pod.Name, container.Name)

						req := clientset.CoreV1().Pods("kueue-system").GetLogs(pod.Name, &corev1.PodLogOptions{
							Container: container.Name,
						})
						logStream, err := req.Stream(context.Background())
						if err != nil {
							fmt.Printf("Failed to get logs for pod %s, container %s: %v\n", pod.Name, container.Name, err)
							continue
						}

						buf := new(bytes.Buffer)
						_, err = io.Copy(buf, logStream)
						logStream.Close()
						if err != nil {
							fmt.Printf("Failed to read logs for pod %s, container %s: %v\n", pod.Name, container.Name, err)
							continue
						}

						fmt.Println(buf.String())
						fmt.Printf("=== End of logs for pod %s, container %s ===\n", pod.Name, container.Name)
					}
				}
			}
			fmt.Println("=== End of Logs for Kueue Controller Pods ===")
		})

		ginkgo.By("Waiting for the RayJob cluster become ready", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayJob := &rayv1.RayJob{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayJob), createdRayJob)).To(gomega.Succeed())
				g.Expect(createdRayJob.Spec.Suspend).To(gomega.BeFalse())
				g.Expect(createdRayJob.Status.JobDeploymentStatus).To(gomega.Equal(rayv1.JobDeploymentStatusRunning))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for 3 pods in rayjob namespace", func() {
			// 3 rayjob pods: head, worker, submitter job
			gomega.Eventually(func(g gomega.Gomega) {
				podList := &corev1.PodList{}
				g.Expect(k8sClient.List(ctx, podList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				g.Expect(podList.Items).To(gomega.HaveLen(3), "Expected exactly 3 pods in rayjob namespace")
				// Get worker pod names and check count
				workerPodNames := getRunningWorkerPodNames(podList)
				g.Expect(workerPodNames).To(gomega.HaveLen(1), "Expected exactly 1 pod with 'workers' in the name")

				// Store initial pod names for later verification
				initialPodNames = workerPodNames
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		// RayJob is top level job, the submitter job created by RayJob will not create its own workload, there will be only 1 workload
		ginkgo.By("Waiting for 1 workloads", func() {
			// 1 workload for the ray cluster
			gomega.Eventually(func(g gomega.Gomega) {
				workloadList := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloadList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				g.Expect(workloadList.Items).To(gomega.HaveLen(1), "Expected exactly 1 workload")
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for 5 workers due to scaling up", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				podList := &corev1.PodList{}
				g.Expect(k8sClient.List(ctx, podList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				// Get worker pod names and check count
				currentPodNames := getRunningWorkerPodNames(podList)
				g.Expect(currentPodNames).To(gomega.HaveLen(5), "Expected exactly 5 pods with 'workers' in the name")

				// Verify that the current pod names are a superset of the initial pod names
				g.Expect(currentPodNames).To(gomega.ContainElements(initialPodNames),
					"Current worker pod names should be a superset of initial pod names")

				// Store scaled-up pod names for later verification during scaling down
				scaledUpPodNames = currentPodNames
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for at least 2 total workloads due to scaling up creating another workload", func() {
			// Use >= 2 since finished slices from intermediate scaling decisions are retained.
			gomega.Eventually(func(g gomega.Gomega) {
				workloadList := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloadList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				g.Expect(len(workloadList.Items)).To(gomega.BeNumerically(">=", 2), "Expected at least 2 workloads")
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Waiting for workers reduced to 1 due to scaling down", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				podList := &corev1.PodList{}
				g.Expect(k8sClient.List(ctx, podList, client.InNamespace(ns.Name))).To(gomega.Succeed())
				// Get worker pod names and check count
				currentPodNames := getRunningWorkerPodNames(podList)
				g.Expect(currentPodNames).To(gomega.HaveLen(1), "Expected exactly 1 pods with 'workers' in the name")

				// Verify that the previous scaled-up pod names are a superset of the current pod names
				g.Expect(scaledUpPodNames).To(gomega.ContainElements(currentPodNames),
					"Previous scaled-up worker pod names should be a superset of current pod names")
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
