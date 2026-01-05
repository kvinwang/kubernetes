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

// kubelet-authorizer is a reference implementation of a Kubelet Authorizer.
// It demonstrates how to implement pod admission control, container lifecycle hooks,
// and API authorization.
//
// Usage:
//   kubelet-authorizer --socket=/var/run/kubelet-authorizer.sock
//
// Then configure kubelet with:
//   --authorizer-socket=/var/run/kubelet-authorizer.sock
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/api/core/v1"
	api "k8s.io/kubernetes/pkg/kubelet/apis/authorizer/v1"
	"k8s.io/kubernetes/pkg/kubelet/authorizer/example"
)

var (
	socketPath       = flag.String("socket", "/var/run/kubelet-authorizer.sock", "Path to the Unix socket")
	denyNamespaces   = flag.String("deny-namespaces", "", "Comma-separated list of namespaces to deny pods from")
	denyImages       = flag.String("deny-images", "", "Comma-separated list of image prefixes to deny")
	requireLabels    = flag.String("require-labels", "", "Comma-separated list of required pod labels")
	exemptNamespaces = flag.String("exempt-namespaces", "kube-system,kube-flannel,kube-public", "Comma-separated list of namespaces exempt from label requirements")
	// API authorization flags
	denyExec         = flag.Bool("deny-exec", false, "Deny all exec requests")
	denyAttach       = flag.Bool("deny-attach", false, "Deny all attach requests")
	denyPortForward  = flag.Bool("deny-portforward", false, "Deny all port-forward requests")
	allowedAPIUsers  = flag.String("allowed-api-users", "", "Comma-separated list of users allowed to use sensitive APIs (empty = allow all)")
)

func main() {
	flag.Parse()

	log.Printf("Starting Kubelet Authorizer")
	log.Printf("Socket path: %s", *socketPath)
	log.Printf("Deny namespaces: %s", *denyNamespaces)
	log.Printf("Deny images: %s", *denyImages)
	log.Printf("Required labels: %s", *requireLabels)
	log.Printf("Exempt namespaces: %s", *exemptNamespaces)
	log.Printf("Deny exec: %v", *denyExec)
	log.Printf("Deny attach: %v", *denyAttach)
	log.Printf("Deny portforward: %v", *denyPortForward)
	log.Printf("Allowed API users: %s", *allowedAPIUsers)

	agent := example.NewAuthorizer(*socketPath)

	// Configure pod admission based on flags
	denyNS := parseList(*denyNamespaces)
	denyImg := parseList(*denyImages)
	reqLabels := parseList(*requireLabels)
	exemptNS := parseList(*exemptNamespaces)
	allowedUsers := parseList(*allowedAPIUsers)

	agent.PodAdmissionHandler = func(pod *v1.Pod, otherPods []*v1.Pod) (bool, string, string) {
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

		// Check required labels (skip for exempt namespaces)
		isExempt := false
		for _, ns := range exemptNS {
			if pod.Namespace == ns {
				isExempt = true
				break
			}
		}
		if !isExempt {
			for _, label := range reqLabels {
				if _, ok := pod.Labels[label]; !ok {
					log.Printf("Pod %s/%s DENIED: missing required label %s", pod.Namespace, pod.Name, label)
					return false, "MissingLabel", "Pod must have label: " + label
				}
			}
		}

		if isExempt {
			log.Printf("Pod %s/%s ALLOWED (exempt namespace)", pod.Namespace, pod.Name)
		} else {
			log.Printf("Pod %s/%s ALLOWED", pod.Namespace, pod.Name)
		}
		return true, "", ""
	}

	// Configure container lifecycle hooks
	agent.ContainerLifecycleHandler = func(pod *v1.Pod, container *v1.Container, containerID string, event api.ContainerLifecycleEvent) (bool, string, map[string]string) {
		podName := "unknown"
		containerName := "unknown"
		if pod != nil {
			podName = pod.Namespace + "/" + pod.Name
		}
		if container != nil {
			containerName = container.Name
		}

		log.Printf("Container lifecycle: %s for pod %s container %s (ID: %s)",
			event.String(), podName, containerName, containerID)

		// Example: collect measurements
		measurements := map[string]string{
			"event":       event.String(),
			"pod":         podName,
			"container":   containerName,
			"containerID": containerID,
		}

		return true, "OK", measurements
	}

	// Configure API authorization
	agent.APIAuthorizationHandler = func(req *api.APIAuthorizationRequest) (bool, string, string) {
		// Check if specific operations are denied
		if *denyExec && strings.HasPrefix(req.Path, "/exec/") {
			log.Printf("API %s %s DENIED: exec disabled", req.Method, req.Path)
			return false, "ExecDisabled", "Exec is disabled on this node"
		}
		if *denyAttach && strings.HasPrefix(req.Path, "/attach/") {
			log.Printf("API %s %s DENIED: attach disabled", req.Method, req.Path)
			return false, "AttachDisabled", "Attach is disabled on this node"
		}
		if *denyPortForward && strings.HasPrefix(req.Path, "/portForward/") {
			log.Printf("API %s %s DENIED: port-forward disabled", req.Method, req.Path)
			return false, "PortForwardDisabled", "Port-forward is disabled on this node"
		}

		// Check allowed users
		if len(allowedUsers) > 0 {
			userAllowed := false
			for _, u := range allowedUsers {
				if u == req.User {
					userAllowed = true
					break
				}
			}
			if !userAllowed {
				log.Printf("API %s %s DENIED: user %s not in allowed list", req.Method, req.Path, req.User)
				return false, "UserNotAllowed", "User " + req.User + " is not allowed to use this API"
			}
		}

		log.Printf("API %s %s ALLOWED (user=%s, pod=%s/%s)", req.Method, req.Path, req.User, req.PodNamespace, req.PodName)
		return true, "", ""
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
