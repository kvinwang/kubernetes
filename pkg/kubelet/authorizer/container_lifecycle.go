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
	"fmt"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
)

// ContainerLifecycle wraps the internal container lifecycle
// and adds Authorizer hooks for measurement and monitoring.
type ContainerLifecycle struct {
	// inner is the wrapped InternalContainerLifecycle
	inner cm.InternalContainerLifecycle
	// client is the Authorizer client
	client AuthorizerClient
	// blockOnFailure determines if container operations should fail
	// when Authorizer is unavailable
	blockOnFailure bool
}

// NewContainerLifecycle creates a new lifecycle wrapper
func NewContainerLifecycle(
	inner cm.InternalContainerLifecycle,
	client AuthorizerClient,
	blockOnFailure bool,
) *ContainerLifecycle {
	return &ContainerLifecycle{
		inner:          inner,
		client:         client,
		blockOnFailure: blockOnFailure,
	}
}

// PreStartContainer is called before a container starts
func (l *ContainerLifecycle) PreStartContainer(pod *v1.Pod, container *v1.Container, containerID string) error {
	glog.V(4).Infof("Authorizer PreStartContainer for %s/%s container %s (ID: %s)",
		pod.Namespace, pod.Name, container.Name, containerID)

	// Call Authorizer first for measurement/authorization
	req := &ContainerLifecycleRequest{
		Pod:         pod,
		Container:   container,
		ContainerID: containerID,
		Event:       ContainerPreStart,
	}

	resp, err := l.client.OnContainerLifecycle(req)
	if err != nil {
		glog.Errorf("Authorizer PreStartContainer failed for %s/%s container %s: %v",
			pod.Namespace, pod.Name, container.Name, err)
		if l.blockOnFailure {
			return fmt.Errorf("Authorizer PreStartContainer failed: %v", err)
		}
		// Continue without blocking
	} else if !resp.Success {
		glog.Warningf("Authorizer PreStartContainer returned failure for %s/%s container %s: %s",
			pod.Namespace, pod.Name, container.Name, resp.Message)
		if l.blockOnFailure {
			return fmt.Errorf("Authorizer rejected PreStartContainer: %s", resp.Message)
		}
	} else {
		// Log any measurements
		if len(resp.Measurements) > 0 {
			glog.V(3).Infof("Authorizer PreStartContainer measurements for %s/%s container %s: %v",
				pod.Namespace, pod.Name, container.Name, resp.Measurements)
		}
	}

	// Then call the inner lifecycle handler (CPU manager, etc.)
	if l.inner != nil {
		return l.inner.PreStartContainer(pod, container, containerID)
	}
	return nil
}

// PreStopContainer is called before a container stops
func (l *ContainerLifecycle) PreStopContainer(containerID string) error {
	glog.V(4).Infof("Authorizer PreStopContainer for container ID: %s", containerID)

	// Call Authorizer for measurement/cleanup
	req := &ContainerLifecycleRequest{
		ContainerID: containerID,
		Event:       ContainerPreStop,
	}

	resp, err := l.client.OnContainerLifecycle(req)
	if err != nil {
		glog.Warningf("Authorizer PreStopContainer failed for container %s: %v", containerID, err)
		// Don't block container stop on Authorizer errors
	} else if !resp.Success {
		glog.Warningf("Authorizer PreStopContainer returned failure for container %s: %s",
			containerID, resp.Message)
	} else if len(resp.Measurements) > 0 {
		glog.V(3).Infof("Authorizer PreStopContainer measurements for container %s: %v",
			containerID, resp.Measurements)
	}

	// Call inner lifecycle handler
	if l.inner != nil {
		return l.inner.PreStopContainer(containerID)
	}
	return nil
}

// PostStopContainer is called after a container stops
func (l *ContainerLifecycle) PostStopContainer(containerID string) error {
	glog.V(4).Infof("Authorizer PostStopContainer for container ID: %s", containerID)

	// Call Authorizer for final measurement/cleanup
	req := &ContainerLifecycleRequest{
		ContainerID: containerID,
		Event:       ContainerPostStop,
	}

	resp, err := l.client.OnContainerLifecycle(req)
	if err != nil {
		glog.Warningf("Authorizer PostStopContainer failed for container %s: %v", containerID, err)
		// Don't block on Authorizer errors
	} else if !resp.Success {
		glog.Warningf("Authorizer PostStopContainer returned failure for container %s: %s",
			containerID, resp.Message)
	} else if len(resp.Measurements) > 0 {
		glog.V(3).Infof("Authorizer PostStopContainer measurements for container %s: %v",
			containerID, resp.Measurements)
	}

	// Call inner lifecycle handler
	if l.inner != nil {
		return l.inner.PostStopContainer(containerID)
	}
	return nil
}

// Ensure ContainerLifecycle implements InternalContainerLifecycle
var _ cm.InternalContainerLifecycle = &ContainerLifecycle{}
