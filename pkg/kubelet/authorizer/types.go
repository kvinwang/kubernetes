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

// Package authorizer provides interfaces for the Kubelet Authorizer
// that allows external authorization services to participate in kubelet's
// policy decisions for pod admission, container lifecycle, and API access.
package authorizer

import (
	"encoding/json"

	"k8s.io/api/core/v1"
)

// JSON serialization helpers for gRPC messages

// MarshalPodJSON serializes a Pod to JSON bytes
func MarshalPodJSON(pod *v1.Pod) ([]byte, error) {
	if pod == nil {
		return nil, nil
	}
	return json.Marshal(pod)
}

// MarshalPodsJSON serializes a slice of Pods to JSON bytes
func MarshalPodsJSON(pods []*v1.Pod) ([]byte, error) {
	if pods == nil {
		return nil, nil
	}
	return json.Marshal(pods)
}

// MarshalContainerJSON serializes a Container to JSON bytes
func MarshalContainerJSON(container *v1.Container) ([]byte, error) {
	if container == nil {
		return nil, nil
	}
	return json.Marshal(container)
}

// PodAdmissionRequest contains the information sent to Authorizer
// for pod admission decision.
type PodAdmissionRequest struct {
	// Pod is the pod being admitted
	Pod *v1.Pod
	// OtherPods are other pods currently on this node
	OtherPods []*v1.Pod
}

// PodAdmissionResponse contains the Authorizer's decision
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

// ContainerLifecycleResponse contains the Authorizer's response
type ContainerLifecycleResponse struct {
	// Success indicates whether the hook succeeded
	Success bool
	// Message is a human-readable message
	Message string
	// Measurements contains any measurements collected by the Authorizer
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

// APIAuthorizationRequest contains information about an incoming API request
type APIAuthorizationRequest struct {
	// Path is the request path (e.g., "/exec", "/attach", "/portForward")
	Path string
	// Method is the HTTP method (GET, POST, etc.)
	Method string
	// PodNamespace is the namespace of the target pod (if applicable)
	PodNamespace string
	// PodName is the name of the target pod (if applicable)
	PodName string
	// ContainerName is the name of the target container (if applicable)
	ContainerName string
	// Command is the command to execute (for exec/run requests)
	Command []string
	// User is the authenticated user making the request
	User string
	// Groups are the groups the user belongs to
	Groups []string
	// SourceIP is the IP address of the client
	SourceIP string
}

// APIAuthorizationResponse contains the Authorizer's decision for API authorization
type APIAuthorizationResponse struct {
	// Allowed indicates whether the request should be allowed
	Allowed bool
	// Reason is a brief CamelCase reason for rejection
	Reason string
	// Message is a human-readable message explaining the decision
	Message string
}

// AuthorizerClient is the interface for communicating with Kubelet Authorizer
type AuthorizerClient interface {
	// CheckPodAdmission checks if a pod should be admitted
	CheckPodAdmission(req *PodAdmissionRequest) (*PodAdmissionResponse, error)

	// OnContainerLifecycle is called on container lifecycle events
	OnContainerLifecycle(req *ContainerLifecycleRequest) (*ContainerLifecycleResponse, error)

	// GetPodMeasurement requests measurements for a pod
	GetPodMeasurement(req *PodMeasurementRequest) (*PodMeasurementResponse, error)

	// CheckAPIAuthorization checks if an incoming API request should be allowed
	CheckAPIAuthorization(req *APIAuthorizationRequest) (*APIAuthorizationResponse, error)

	// Close closes the connection to the Authorizer
	Close() error
}
