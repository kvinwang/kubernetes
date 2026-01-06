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
	"k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm"
)

// ContainerLifecycle wraps the internal container lifecycle and adds Authorizer hooks
type ContainerLifecycle struct {
	inner    cm.InternalContainerLifecycle
	client   *Client
	failOpen bool
}

// NewContainerLifecycle creates a new ContainerLifecycle that wraps the inner lifecycle
func NewContainerLifecycle(inner cm.InternalContainerLifecycle, client *Client, failOpen bool) *ContainerLifecycle {
	return &ContainerLifecycle{
		inner:    inner,
		client:   client,
		failOpen: failOpen,
	}
}

// PreCreateContainer is called before a container is created
func (l *ContainerLifecycle) PreCreateContainer(pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	// Call inner first
	if err := l.inner.PreCreateContainer(pod, container, containerConfig); err != nil {
		return err
	}

	// Call Authorizer hook if client is available
	if l.client != nil {
		req := &ContainerLifecycleRequest{
			Pod:       pod,
			Container: container,
			Event:     ContainerPreStart,
		}
		resp, err := l.client.OnContainerLifecycle(req)
		if err != nil {
			klog.ErrorS(err, "Authorizer PreCreateContainer hook failed", "pod", pod.Name, "container", container.Name)
			if !l.failOpen {
				return err
			}
		} else if !resp.Success {
			klog.InfoS("Authorizer PreCreateContainer returned failure", "pod", pod.Name, "container", container.Name, "message", resp.Message)
		}
	}

	return nil
}

// PreStartContainer is called before a container is started
func (l *ContainerLifecycle) PreStartContainer(pod *v1.Pod, container *v1.Container, containerID string) error {
	// Call inner first
	if err := l.inner.PreStartContainer(pod, container, containerID); err != nil {
		return err
	}

	// Call Authorizer hook if client is available
	if l.client != nil {
		req := &ContainerLifecycleRequest{
			Pod:         pod,
			Container:   container,
			ContainerID: containerID,
			Event:       ContainerPostStart,
		}
		resp, err := l.client.OnContainerLifecycle(req)
		if err != nil {
			klog.ErrorS(err, "Authorizer PreStartContainer hook failed", "pod", pod.Name, "container", container.Name)
			if !l.failOpen {
				return err
			}
		} else if !resp.Success {
			klog.InfoS("Authorizer PreStartContainer returned failure", "pod", pod.Name, "container", container.Name, "message", resp.Message)
		}
	}

	return nil
}

// PostStopContainer is called after a container is stopped
func (l *ContainerLifecycle) PostStopContainer(containerID string) error {
	// Call Authorizer hook if client is available
	if l.client != nil {
		req := &ContainerLifecycleRequest{
			ContainerID: containerID,
			Event:       ContainerPostStop,
		}
		resp, err := l.client.OnContainerLifecycle(req)
		if err != nil {
			klog.ErrorS(err, "Authorizer PostStopContainer hook failed", "containerID", containerID)
			// Don't fail on post-stop hooks
		} else if !resp.Success {
			klog.InfoS("Authorizer PostStopContainer returned failure", "containerID", containerID, "message", resp.Message)
		}
	}

	// Call inner
	return l.inner.PostStopContainer(containerID)
}

// Verify ContainerLifecycle implements cm.InternalContainerLifecycle
var _ cm.InternalContainerLifecycle = &ContainerLifecycle{}
