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

package authorizer

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// PodAdmitHandler implements lifecycle.PodAdmitHandler for Authorizer
type PodAdmitHandler struct {
	client   *Client
	failOpen bool
}

// NewPodAdmitHandler creates a new PodAdmitHandler
func NewPodAdmitHandler(client *Client, failOpen bool) *PodAdmitHandler {
	return &PodAdmitHandler{
		client:   client,
		failOpen: failOpen,
	}
}

// Admit evaluates if a pod can be admitted
func (h *PodAdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	pod := attrs.Pod

	// If no client is configured, allow the pod
	if h.client == nil {
		return lifecycle.PodAdmitResult{
			Admit: true,
		}
	}

	// Call Authorizer to check pod admission
	req := &PodAdmissionRequest{
		Pod:       pod,
		OtherPods: attrs.OtherPods,
	}

	resp, err := h.client.CheckPodAdmission(req)
	if err != nil {
		klog.ErrorS(err, "Authorizer pod admission check failed", "pod", pod.Name, "namespace", pod.Namespace)
		if h.failOpen {
			return lifecycle.PodAdmitResult{
				Admit:   true,
				Reason:  "AuthorizerUnavailable",
				Message: "Authorizer unavailable, allowing pod",
			}
		}
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  "AuthorizerUnavailable",
			Message: "Authorizer unavailable and fail-open is disabled",
		}
	}

	if !resp.Allowed {
		klog.InfoS("Authorizer denied pod admission", "pod", pod.Name, "namespace", pod.Namespace, "reason", resp.Reason, "message", resp.Message)
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  resp.Reason,
			Message: resp.Message,
		}
	}

	klog.V(4).InfoS("Authorizer allowed pod admission", "pod", pod.Name, "namespace", pod.Namespace)
	return lifecycle.PodAdmitResult{
		Admit: true,
	}
}

// Verify PodAdmitHandler implements lifecycle.PodAdmitHandler
var _ lifecycle.PodAdmitHandler = &PodAdmitHandler{}
