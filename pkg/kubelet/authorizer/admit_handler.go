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
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// AdmitHandler implements lifecycle.PodAdmitHandler
// It delegates pod admission decisions to the Kubelet Authorizer.
type AdmitHandler struct {
	client AuthorizerClient
	// failOpen determines behavior when Authorizer is unavailable
	// If true (default), pods are admitted when Authorizer is unavailable
	// If false, pods are rejected when Authorizer is unavailable
	failOpen bool
}

// NewAdmitHandler creates a new Authorizer admit handler
func NewAdmitHandler(client AuthorizerClient, failOpen bool) *AdmitHandler {
	return &AdmitHandler{
		client:   client,
		failOpen: failOpen,
	}
}

// Admit evaluates if a pod can be admitted by consulting the Authorizer
func (h *AdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	pod := attrs.Pod

	glog.V(4).Infof("Checking Authorizer admission for pod %s/%s", pod.Namespace, pod.Name)

	req := &PodAdmissionRequest{
		Pod:       pod,
		OtherPods: attrs.OtherPods,
	}

	resp, err := h.client.CheckPodAdmission(req)
	if err != nil {
		glog.Errorf("Authorizer admission check failed for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		if h.failOpen {
			return lifecycle.PodAdmitResult{
				Admit:   true,
				Reason:  "AuthorizerUnavailable",
				Message: "Authorizer unavailable, pod admitted (fail-open mode)",
			}
		}
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  "AuthorizerUnavailable",
			Message: "Authorizer unavailable, pod rejected (fail-closed mode)",
		}
	}

	if !resp.Allowed {
		glog.V(2).Infof("Authorizer rejected pod %s/%s: %s - %s", pod.Namespace, pod.Name, resp.Reason, resp.Message)
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  resp.Reason,
			Message: resp.Message,
		}
	}

	glog.V(4).Infof("Authorizer admitted pod %s/%s", pod.Namespace, pod.Name)
	return lifecycle.PodAdmitResult{Admit: true}
}

var _ lifecycle.PodAdmitHandler = &AdmitHandler{}
