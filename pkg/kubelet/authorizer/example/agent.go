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

// Package example provides a reference implementation of a Kubelet Authorizer
// gRPC server that can be used for testing and as a starting point for custom authorizers.
package example

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/api/core/v1"

	api "k8s.io/kubernetes/pkg/kubelet/apis/authorizer/v1"
)

// Authorizer is a reference implementation of a Kubelet Authorizer gRPC server
type Authorizer struct {
	socketPath string
	listener   net.Listener
	server     *grpc.Server
	mu         sync.Mutex
	running    bool

	// Callbacks for customization
	PodAdmissionHandler       func(pod *v1.Pod, otherPods []*v1.Pod) (allowed bool, reason, message string)
	ContainerLifecycleHandler func(pod *v1.Pod, container *v1.Container, containerID string, event api.ContainerLifecycleEvent) (success bool, message string, measurements map[string]string)
	PodMeasurementHandler     func(pod *v1.Pod, containerID string) (measurements map[string]string, err error)
	APIAuthorizationHandler   func(req *api.APIAuthorizationRequest) (allowed bool, reason, message string)
}

// NewAuthorizer creates a new Authorizer with default callbacks
func NewAuthorizer(socketPath string) *Authorizer {
	return &Authorizer{
		socketPath: socketPath,
		PodAdmissionHandler: func(pod *v1.Pod, otherPods []*v1.Pod) (bool, string, string) {
			log.Printf("Pod admission check for %s/%s - ALLOWED", pod.Namespace, pod.Name)
			return true, "", ""
		},
		ContainerLifecycleHandler: func(pod *v1.Pod, container *v1.Container, containerID string, event api.ContainerLifecycleEvent) (bool, string, map[string]string) {
			podName := "unknown"
			containerName := "unknown"
			if pod != nil {
				podName = pod.Namespace + "/" + pod.Name
			}
			if container != nil {
				containerName = container.Name
			}
			log.Printf("Container lifecycle event: %s for %s container %s (ID: %s)", event, podName, containerName, containerID)
			return true, "OK", map[string]string{
				"timestamp": time.Now().Format(time.RFC3339),
				"event":     event.String(),
			}
		},
		PodMeasurementHandler: func(pod *v1.Pod, containerID string) (map[string]string, error) {
			return map[string]string{
				"timestamp": time.Now().Format(time.RFC3339),
				"pod":       pod.Namespace + "/" + pod.Name,
			}, nil
		},
		APIAuthorizationHandler: func(req *api.APIAuthorizationRequest) (bool, string, string) {
			log.Printf("API authorization: %s %s for pod %s/%s by user %s - ALLOWED",
				req.Method, req.Path, req.PodNamespace, req.PodName, req.User)
			return true, "", ""
		},
	}
}

// Start starts the gRPC server
func (a *Authorizer) Start() error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("authorizer already running")
	}

	network := "unix"
	address := a.socketPath
	if len(a.socketPath) > 0 && a.socketPath[0] != '/' && a.socketPath[0] != '.' {
		network = "tcp"
	} else {
		os.Remove(a.socketPath)
	}

	listener, err := net.Listen(network, address)
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("failed to listen on %s (%s): %v", address, network, err)
	}

	a.listener = listener
	a.server = grpc.NewServer()
	api.RegisterAuthorizerServiceServer(a.server, a)
	a.running = true
	a.mu.Unlock()

	log.Printf("Authorizer gRPC server listening on %s (%s)", address, network)
	return a.server.Serve(listener)
}

// Stop stops the gRPC server
func (a *Authorizer) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}

	a.running = false
	if a.server != nil {
		a.server.GracefulStop()
	}
	if a.socketPath[0] == '/' || a.socketPath[0] == '.' {
		os.Remove(a.socketPath)
	}
	return nil
}

// CheckPodAdmission implements the gRPC service method
func (a *Authorizer) CheckPodAdmission(ctx context.Context, req *api.PodAdmissionRequest) (*api.PodAdmissionResponse, error) {
	var pod v1.Pod
	if err := json.Unmarshal(req.PodJson, &pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pod: %v", err)
	}

	var otherPods []*v1.Pod
	if len(req.OtherPodsJson) > 0 {
		if err := json.Unmarshal(req.OtherPodsJson, &otherPods); err != nil {
			return nil, fmt.Errorf("failed to unmarshal other pods: %v", err)
		}
	}

	allowed, reason, message := a.PodAdmissionHandler(&pod, otherPods)
	return &api.PodAdmissionResponse{
		Allowed: allowed,
		Reason:  reason,
		Message: message,
	}, nil
}

// OnContainerLifecycle implements the gRPC service method
func (a *Authorizer) OnContainerLifecycle(ctx context.Context, req *api.ContainerLifecycleRequest) (*api.ContainerLifecycleResponse, error) {
	var pod *v1.Pod
	if len(req.PodJson) > 0 {
		pod = &v1.Pod{}
		if err := json.Unmarshal(req.PodJson, pod); err != nil {
			return nil, fmt.Errorf("failed to unmarshal pod: %v", err)
		}
	}

	var container *v1.Container
	if len(req.ContainerJson) > 0 {
		container = &v1.Container{}
		if err := json.Unmarshal(req.ContainerJson, container); err != nil {
			return nil, fmt.Errorf("failed to unmarshal container: %v", err)
		}
	}

	success, message, measurements := a.ContainerLifecycleHandler(pod, container, req.ContainerId, req.Event)
	return &api.ContainerLifecycleResponse{
		Success:      success,
		Message:      message,
		Measurements: measurements,
	}, nil
}

// GetPodMeasurement implements the gRPC service method
func (a *Authorizer) GetPodMeasurement(ctx context.Context, req *api.PodMeasurementRequest) (*api.PodMeasurementResponse, error) {
	var pod v1.Pod
	if err := json.Unmarshal(req.PodJson, &pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pod: %v", err)
	}

	measurements, err := a.PodMeasurementHandler(&pod, req.ContainerId)
	if err != nil {
		return &api.PodMeasurementResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &api.PodMeasurementResponse{
		Success:      true,
		Measurements: measurements,
	}, nil
}

// CheckAPIAuthorization implements the gRPC service method
func (a *Authorizer) CheckAPIAuthorization(ctx context.Context, req *api.APIAuthorizationRequest) (*api.APIAuthorizationResponse, error) {
	allowed, reason, message := a.APIAuthorizationHandler(req)
	return &api.APIAuthorizationResponse{
		Allowed: allowed,
		Reason:  reason,
		Message: message,
	}, nil
}

// Ensure Authorizer implements the gRPC service interface
var _ api.AuthorizerServiceServer = &Authorizer{}
