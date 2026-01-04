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

// Package externalagent provides interfaces for external agent hooks
// that allow external processes to participate in kubelet's pod lifecycle
// decisions and measurements.
package externalagent

import (
	"k8s.io/api/core/v1"
)

// PodAdmissionRequest contains the information sent to external agent
// for pod admission decision.
type PodAdmissionRequest struct {
	// Pod is the pod being admitted
	Pod *v1.Pod
	// OtherPods are other pods currently on this node
	OtherPods []*v1.Pod
}

// PodAdmissionResponse contains the external agent's decision
type PodAdmissionResponse struct {
	// Allowed indicates whether the pod should be admitted
	Allowed bool
	// Reason is a brief CamelCase reason for rejection
	Reason string
	// Message is a human-readable message explaining the decision
	Message string
}

// ContainerLifecycleRequest contains container lifecycle event information
type ContainerLifecycleRequest struct {
	// Pod is the pod containing the container
	Pod *v1.Pod
	// Container is the container spec
	Container *v1.Container
	// ContainerID is the runtime container ID
	ContainerID string
	// Event is the lifecycle event type
	Event ContainerLifecycleEvent
}

// ContainerLifecycleEvent represents a container lifecycle event
type ContainerLifecycleEvent string

const (
	// ContainerPreStart is called before container starts
	ContainerPreStart ContainerLifecycleEvent = "PreStart"
	// ContainerPostStart is called after container starts
	ContainerPostStart ContainerLifecycleEvent = "PostStart"
	// ContainerPreStop is called before container stops
	ContainerPreStop ContainerLifecycleEvent = "PreStop"
	// ContainerPostStop is called after container stops
	ContainerPostStop ContainerLifecycleEvent = "PostStop"
)

// ContainerLifecycleResponse contains the external agent's response
type ContainerLifecycleResponse struct {
	// Success indicates whether the hook succeeded
	Success bool
	// Message is a human-readable message
	Message string
	// Measurements contains any measurements collected by the agent
	Measurements map[string]string
}

// PodMeasurementRequest is sent to request pod measurements
type PodMeasurementRequest struct {
	// Pod is the pod to measure
	Pod *v1.Pod
	// ContainerID is optional, for container-specific measurements
	ContainerID string
}

// PodMeasurementResponse contains measurement results
type PodMeasurementResponse struct {
	// Success indicates whether the measurement succeeded
	Success bool
	// Measurements is a map of measurement name to value
	Measurements map[string]string
	// Message is a human-readable message
	Message string
}

// ExternalAgentClient is the interface for communicating with external agent
type ExternalAgentClient interface {
	// CheckPodAdmission checks if a pod should be admitted
	CheckPodAdmission(req *PodAdmissionRequest) (*PodAdmissionResponse, error)

	// OnContainerLifecycle is called on container lifecycle events
	OnContainerLifecycle(req *ContainerLifecycleRequest) (*ContainerLifecycleResponse, error)

	// GetPodMeasurement requests measurements for a pod
	GetPodMeasurement(req *PodMeasurementRequest) (*PodMeasurementResponse, error)

	// Close closes the connection to the agent
	Close() error
}
