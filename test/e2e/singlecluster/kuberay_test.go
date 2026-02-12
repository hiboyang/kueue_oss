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
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	workloadraycluster "sigs.k8s.io/kueue/pkg/controller/jobs/raycluster"
	workloadrayjob "sigs.k8s.io/kueue/pkg/controller/jobs/rayjob"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingraycluster "sigs.k8s.io/kueue/pkg/util/testingjobs/raycluster"
	testingrayjob "sigs.k8s.io/kueue/pkg/util/testingjobs/rayjob"
	testingrayservice "sigs.k8s.io/kueue/pkg/util/testingjobs/rayservice"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Kuberay", ginkgo.Label("area:singlecluster", "feature:kuberay"), func() {
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

	// dumpDebugInfoOnFailure prints debug information (YAMLs and logs) when a test fails
	dumpDebugInfoOnFailure := func(namespace string) {
		if !ginkgo.CurrentSpecReport().Failed() {
			return
		}

		// Print RayServices
		rayServiceList := &rayv1.RayServiceList{}
		if err := k8sClient.List(ctx, rayServiceList, client.InNamespace(namespace)); err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to list RayServices: %v\n", err)
		} else {
			for _, rs := range rayServiceList.Items {
				rs.ManagedFields = nil
				rsYAML, err := yaml.Marshal(rs)
				if err != nil {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to marshal RayService %s: %v\n", rs.Name, err)
					continue
				}
				fmt.Fprintf(ginkgo.GinkgoWriter, "=== RayService %s/%s YAML ===\n%s\n", rs.Namespace, rs.Name, string(rsYAML))
			}
		}

		// Print Workloads
		workloadList := &kueue.WorkloadList{}
		if err := k8sClient.List(ctx, workloadList, client.InNamespace(namespace)); err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to list Workloads: %v\n", err)
		} else {
			for _, wl := range workloadList.Items {
				wl.ManagedFields = nil
				wlYAML, err := yaml.Marshal(wl)
				if err != nil {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to marshal Workload %s: %v\n", wl.Name, err)
					continue
				}
				fmt.Fprintf(ginkgo.GinkgoWriter, "=== Workload %s/%s YAML ===\n%s\n", wl.Namespace, wl.Name, string(wlYAML))
			}
		}

		// Print Jobs
		jobList := &batchv1.JobList{}
		if err := k8sClient.List(ctx, jobList, client.InNamespace(namespace)); err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to list Jobs: %v\n", err)
		} else {
			for _, job := range jobList.Items {
				job.ManagedFields = nil
				jobYAML, err := yaml.Marshal(job)
				if err != nil {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to marshal Job %s: %v\n", job.Name, err)
					continue
				}
				fmt.Fprintf(ginkgo.GinkgoWriter, "=== Job %s/%s YAML ===\n%s\n", job.Namespace, job.Name, string(jobYAML))
			}
		}

		// Print Pods in namespace
		podList := &corev1.PodList{}
		if err := k8sClient.List(ctx, podList, client.InNamespace(namespace)); err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to list Pods: %v\n", err)
		} else {
			for _, pod := range podList.Items {
				pod.ManagedFields = nil
				podYAML, err := yaml.Marshal(pod)
				if err != nil {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to marshal Pod %s: %v\n", pod.Name, err)
					continue
				}
				fmt.Fprintf(ginkgo.GinkgoWriter, "=== Pod %s/%s YAML ===\n%s\n", pod.Namespace, pod.Name, string(podYAML))
			}
		}

		// Print logs for all pods in the cluster with "ray" or "kueue" in their name
		allPodList := &corev1.PodList{}
		if err := k8sClient.List(ctx, allPodList); err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to list all Pods in cluster: %v\n", err)
		} else {
			for _, pod := range allPodList.Items {
				podNameLower := strings.ToLower(pod.Name)
				if !strings.Contains(podNameLower, "ray") && !strings.Contains(podNameLower, "kueue") {
					continue
				}
				for _, container := range pod.Spec.Containers {
					logs, err := util.GetPodLogs(ctx, cfg, &pod, container.Name)
					if err != nil {
						fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get logs for Pod %s/%s container %s: %v\n",
							pod.Namespace, pod.Name, container.Name, err)
						continue
					}
					fmt.Fprintf(ginkgo.GinkgoWriter, "=== Logs for Pod %s/%s container %s ===\n%s\n",
						pod.Namespace, pod.Name, container.Name, logs)
				}
			}
		}
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
print([ray.get(my_task.remote(i, 1)) for i in range(32)])`,
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
					if workload.IsAdmitted(&wl) || workload.IsFinished(&wl) {
						hasAdmittedWorkload = true
						break
					}
				}
				g.Expect(hasAdmittedWorkload).To(gomega.BeTrue(), "Expected admitted workload")
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
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

	ginkgo.It("Should run a RayService if admitted", func() {
		kuberayTestImage := util.GetKuberayTestImage()

		rayService := testingrayservice.MakeService("rayservice", ns.Name).
			Suspend(true).
			Queue(localQueueName).
			RequestAndLimit(rayv1.HeadNode, corev1.ResourceCPU, "300m").
			RequestAndLimit(rayv1.WorkerNode, corev1.ResourceCPU, "300m").
			Image(rayv1.HeadNode, kuberayTestImage).
			Image(rayv1.WorkerNode, kuberayTestImage).
			Obj()

		// Print debug info on test failure
		ginkgo.DeferCleanup(func() {
			dumpDebugInfoOnFailure(ns.Name)
		})

		ginkgo.By("Creating the RayService", func() {
			gomega.Expect(k8sClient.Create(ctx, rayService)).Should(gomega.Succeed())
		})

		// RayService creates an underlying RayCluster, and the workload is created for that RayCluster
		var rayCluster *rayv1.RayCluster
		ginkgo.By("Waiting for RayService to create a RayCluster", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayService := &rayv1.RayService{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayService), createdRayService)).To(gomega.Succeed())
				rayClusterName := createdRayService.Status.PendingServiceStatus.RayClusterName
				if rayClusterName == "" {
					rayClusterName = createdRayService.Status.ActiveServiceStatus.RayClusterName
				}
				g.Expect(rayClusterName).NotTo(gomega.BeEmpty(), "RayCluster name should be set in RayService status")

				rayCluster = &rayv1.RayCluster{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rayClusterName, Namespace: ns.Name}, rayCluster)).To(gomega.Succeed())
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		createdWorkload := &kueue.Workload{}
		ginkgo.By("Checking workload is created for the RayCluster", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				wlLookupKey := types.NamespacedName{
					Name:      workloadraycluster.GetWorkloadNameForRayCluster(rayCluster.Name, rayCluster.UID),
					Namespace: ns.Name,
				}
				g.Expect(k8sClient.Get(ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("Checking workload is admitted", func() {
			util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, createdWorkload)
		})

		ginkgo.By("Checking the RayService is running", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdRayService := &rayv1.RayService{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rayService), createdRayService)).To(gomega.Succeed())
				g.Expect(createdRayService.Spec.RayClusterSpec.Suspend).To(gomega.Equal(ptr.To(false)))
				g.Expect(createdRayService.Status.ServiceStatus).To(gomega.Equal(rayv1.Running))
			}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
		})
	})
})
