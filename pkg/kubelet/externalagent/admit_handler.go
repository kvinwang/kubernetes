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

package externalagent

import (
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// ExternalAgentAdmitHandler implements lifecycle.PodAdmitHandler
// It delegates pod admission decisions to an external agent process.
type ExternalAgentAdmitHandler struct {
	client ExternalAgentClient
	// failOpen determines behavior when agent is unavailable
	// If true (default), pods are admitted when agent is unavailable
	// If false, pods are rejected when agent is unavailable
	failOpen bool
}

// NewExternalAgentAdmitHandler creates a new external agent admit handler
func NewExternalAgentAdmitHandler(client ExternalAgentClient, failOpen bool) *ExternalAgentAdmitHandler {
	return &ExternalAgentAdmitHandler{
		client:   client,
		failOpen: failOpen,
	}
}

// Admit evaluates if a pod can be admitted by consulting the external agent
func (h *ExternalAgentAdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	pod := attrs.Pod

	glog.V(4).Infof("Checking external agent admission for pod %s/%s", pod.Namespace, pod.Name)

	req := &PodAdmissionRequest{
		Pod:       pod,
		OtherPods: attrs.OtherPods,
	}

	resp, err := h.client.CheckPodAdmission(req)
	if err != nil {
		glog.Errorf("External agent admission check failed for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		if h.failOpen {
			return lifecycle.PodAdmitResult{
				Admit:   true,
				Reason:  "ExternalAgentUnavailable",
				Message: "External agent unavailable, pod admitted (fail-open mode)",
			}
		}
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  "ExternalAgentUnavailable",
			Message: "External agent unavailable, pod rejected (fail-closed mode)",
		}
	}

	if !resp.Allowed {
		glog.V(2).Infof("External agent rejected pod %s/%s: %s - %s", pod.Namespace, pod.Name, resp.Reason, resp.Message)
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  resp.Reason,
			Message: resp.Message,
		}
	}

	glog.V(4).Infof("External agent admitted pod %s/%s", pod.Namespace, pod.Name)
	return lifecycle.PodAdmitResult{Admit: true}
}

var _ lifecycle.PodAdmitHandler = &ExternalAgentAdmitHandler{}
