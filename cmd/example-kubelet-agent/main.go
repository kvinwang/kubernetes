/*
Copyright 2018 The Kubernetes Authors.

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

// example-kubelet-agent is a reference implementation of an external kubelet agent.
// It demonstrates how to implement pod admission control and container lifecycle hooks.
//
// Usage:
//   example-kubelet-agent --socket=/var/run/kubelet-agent.sock
//
// Then configure kubelet with:
//   --external-agent-socket=/var/run/kubelet-agent.sock
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/externalagent/example"
)

var (
	socketPath       = flag.String("socket", "/var/run/kubelet-agent.sock", "Path to the Unix socket")
	denyNamespaces   = flag.String("deny-namespaces", "", "Comma-separated list of namespaces to deny pods from")
	denyImages       = flag.String("deny-images", "", "Comma-separated list of image prefixes to deny")
	requireLabels    = flag.String("require-labels", "", "Comma-separated list of required pod labels")
)

func main() {
	flag.Parse()

	log.Printf("Starting example kubelet agent")
	log.Printf("Socket path: %s", *socketPath)
	log.Printf("Deny namespaces: %s", *denyNamespaces)
	log.Printf("Deny images: %s", *denyImages)
	log.Printf("Required labels: %s", *requireLabels)

	agent := example.NewExampleAgent(*socketPath)

	// Configure pod admission based on flags
	denyNS := parseList(*denyNamespaces)
	denyImg := parseList(*denyImages)
	reqLabels := parseList(*requireLabels)

	agent.OnPodAdmission = func(pod *v1.Pod, otherPods []*v1.Pod) (bool, string, string) {
		// Check denied namespaces
		for _, ns := range denyNS {
			if pod.Namespace == ns {
				log.Printf("Pod %s/%s DENIED: namespace %s is blocked", pod.Namespace, pod.Name, ns)
				return false, "NamespaceBlocked", "Namespace " + ns + " is not allowed on this node"
			}
		}

		// Check denied image prefixes
		for _, container := range pod.Spec.Containers {
			for _, imgPrefix := range denyImg {
				if strings.HasPrefix(container.Image, imgPrefix) {
					log.Printf("Pod %s/%s DENIED: image %s is blocked", pod.Namespace, pod.Name, container.Image)
					return false, "ImageBlocked", "Image " + container.Image + " is not allowed on this node"
				}
			}
		}

		// Check required labels
		for _, label := range reqLabels {
			if _, ok := pod.Labels[label]; !ok {
				log.Printf("Pod %s/%s DENIED: missing required label %s", pod.Namespace, pod.Name, label)
				return false, "MissingLabel", "Pod must have label: " + label
			}
		}

		log.Printf("Pod %s/%s ALLOWED", pod.Namespace, pod.Name)
		return true, "", ""
	}

	// Configure container lifecycle hooks
	agent.OnContainerLifecycle = func(pod *v1.Pod, container *v1.Container, containerID, event string) (bool, string, map[string]string) {
		podName := "unknown"
		containerName := "unknown"
		if pod != nil {
			podName = pod.Namespace + "/" + pod.Name
		}
		if container != nil {
			containerName = container.Name
		}

		log.Printf("Container lifecycle: %s for pod %s container %s (ID: %s)",
			event, podName, containerName, containerID)

		// Example: collect measurements
		measurements := map[string]string{
			"event":       event,
			"pod":         podName,
			"container":   containerName,
			"containerID": containerID,
		}

		return true, "OK", measurements
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("Shutting down...")
		agent.Stop()
		os.Exit(0)
	}()

	// Start the agent
	if err := agent.Start(); err != nil {
		log.Fatalf("Failed to start agent: %v", err)
	}
}

func parseList(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}
