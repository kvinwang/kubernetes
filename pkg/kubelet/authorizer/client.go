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
	"context"
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	api "k8s.io/kubernetes/pkg/kubelet/apis/authorizer/v1"
)

const (
	// DefaultTimeout is the default timeout for Authorizer calls
	DefaultTimeout = 30 * time.Second
)

// Client implements AuthorizerClient using gRPC
type Client struct {
	conn    *grpc.ClientConn
	client  api.AuthorizerServiceClient
	timeout time.Duration
}

// Alias for backwards compatibility
type SocketClient = Client

// NewClient creates a new gRPC Authorizer client
// If address starts with "/" or ".", it's a Unix socket; otherwise TCP
func NewClient(address string, timeout time.Duration) (*Client, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var conn *grpc.ClientConn
	var err error

	if len(address) > 0 && (address[0] == '/' || address[0] == '.') {
		// Unix socket
		conn, err = grpc.DialContext(ctx, address,
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
				return net.DialTimeout("unix", addr, timeout)
			}),
		)
	} else {
		// TCP
		conn, err = grpc.DialContext(ctx, address,
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Authorizer at %s: %v", address, err)
	}

	return &Client{
		conn:    conn,
		client:  api.NewAuthorizerServiceClient(conn),
		timeout: timeout,
	}, nil
}

// CheckPodAdmission checks if a pod should be admitted
func (c *Client) CheckPodAdmission(req *PodAdmissionRequest) (*PodAdmissionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	podJSON, err := MarshalPodJSON(req.Pod)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pod: %v", err)
	}

	otherPodsJSON, err := MarshalPodsJSON(req.OtherPods)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal other pods: %v", err)
	}

	grpcReq := &api.PodAdmissionRequest{
		PodJson:       podJSON,
		OtherPodsJson: otherPodsJSON,
	}

	resp, err := c.client.CheckPodAdmission(ctx, grpcReq)
	if err != nil {
		glog.Errorf("Authorizer pod admission check failed: %v", err)
		// On error, allow the pod (fail-open policy)
		return &PodAdmissionResponse{
			Allowed: true,
			Reason:  "AuthorizerUnavailable",
			Message: fmt.Sprintf("Authorizer unavailable, allowing pod: %v", err),
		}, nil
	}

	return &PodAdmissionResponse{
		Allowed: resp.Allowed,
		Reason:  resp.Reason,
		Message: resp.Message,
	}, nil
}

// OnContainerLifecycle is called on container lifecycle events
func (c *Client) OnContainerLifecycle(req *ContainerLifecycleRequest) (*ContainerLifecycleResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	podJSON, err := MarshalPodJSON(req.Pod)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pod: %v", err)
	}

	containerJSON, err := MarshalContainerJSON(req.Container)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal container: %v", err)
	}

	grpcReq := &api.ContainerLifecycleRequest{
		PodJson:       podJSON,
		ContainerJson: containerJSON,
		ContainerId:   req.ContainerID,
		Event:         convertLifecycleEvent(req.Event),
	}

	resp, err := c.client.OnContainerLifecycle(ctx, grpcReq)
	if err != nil {
		glog.Warningf("Authorizer container lifecycle hook failed: %v", err)
		return &ContainerLifecycleResponse{
			Success: true,
			Message: fmt.Sprintf("Authorizer unavailable: %v", err),
		}, nil
	}

	return &ContainerLifecycleResponse{
		Success:      resp.Success,
		Message:      resp.Message,
		Measurements: resp.Measurements,
	}, nil
}

// GetPodMeasurement requests measurements for a pod
func (c *Client) GetPodMeasurement(req *PodMeasurementRequest) (*PodMeasurementResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	podJSON, err := MarshalPodJSON(req.Pod)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pod: %v", err)
	}

	grpcReq := &api.PodMeasurementRequest{
		PodJson:     podJSON,
		ContainerId: req.ContainerID,
	}

	resp, err := c.client.GetPodMeasurement(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("pod measurement request failed: %v", err)
	}

	return &PodMeasurementResponse{
		Success:      resp.Success,
		Measurements: resp.Measurements,
		Message:      resp.Message,
	}, nil
}

// CheckAPIAuthorization checks if an incoming API request should be allowed
func (c *Client) CheckAPIAuthorization(req *APIAuthorizationRequest) (*APIAuthorizationResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	grpcReq := &api.APIAuthorizationRequest{
		Path:          req.Path,
		Method:        req.Method,
		PodNamespace:  req.PodNamespace,
		PodName:       req.PodName,
		ContainerName: req.ContainerName,
		Command:       req.Command,
		User:          req.User,
		Groups:        req.Groups,
		SourceIp:      req.SourceIP,
	}

	resp, err := c.client.CheckAPIAuthorization(ctx, grpcReq)
	if err != nil {
		glog.Errorf("Authorizer API authorization check failed: %v", err)
		return &APIAuthorizationResponse{
			Allowed: true,
			Reason:  "AuthorizerUnavailable",
			Message: fmt.Sprintf("Authorizer unavailable, allowing request: %v", err),
		}, nil
	}

	return &APIAuthorizationResponse{
		Allowed: resp.Allowed,
		Reason:  resp.Reason,
		Message: resp.Message,
	}, nil
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Helper to convert lifecycle event to proto enum
func convertLifecycleEvent(event ContainerLifecycleEvent) api.ContainerLifecycleEvent {
	switch event {
	case ContainerPreStart:
		return api.ContainerLifecycleEvent_PRE_START
	case ContainerPostStart:
		return api.ContainerLifecycleEvent_POST_START
	case ContainerPreStop:
		return api.ContainerLifecycleEvent_PRE_STOP
	case ContainerPostStop:
		return api.ContainerLifecycleEvent_POST_STOP
	default:
		return api.ContainerLifecycleEvent_UNKNOWN
	}
}
