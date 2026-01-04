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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	// DefaultTimeout is the default timeout for agent calls
	DefaultTimeout = 30 * time.Second
	// MaxMessageSize is the maximum message size in bytes
	MaxMessageSize = 10 * 1024 * 1024 // 10MB
)

// UnixSocketClient implements ExternalAgentClient using Unix domain socket
type UnixSocketClient struct {
	socketPath string
	timeout    time.Duration
	mu         sync.Mutex
	conn       net.Conn
}

// NewUnixSocketClient creates a new Unix socket client
func NewUnixSocketClient(socketPath string, timeout time.Duration) (*UnixSocketClient, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	client := &UnixSocketClient{
		socketPath: socketPath,
		timeout:    timeout,
	}
	// Test connection
	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to external agent at %s: %v", socketPath, err)
	}
	return client, nil
}

func (c *UnixSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := net.DialTimeout("unix", c.socketPath, c.timeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Message types for the protocol
const (
	MsgTypePodAdmission        = "pod_admission"
	MsgTypeContainerLifecycle  = "container_lifecycle"
	MsgTypePodMeasurement      = "pod_measurement"
)

// AgentMessage is the wire format for messages
type AgentMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// AgentResponse is the wire format for responses
type AgentResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

func (c *UnixSocketClient) sendRequest(ctx context.Context, msgType string, request interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Serialize payload
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	msg := AgentMessage{
		Type:    msgType,
		Payload: payload,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	// Set deadline
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.timeout)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		// Try to reconnect
		if err := c.reconnect(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %v", err)
		}
		c.conn.SetDeadline(deadline)
	}

	// Send length-prefixed message
	lenBuf := make([]byte, 4)
	msgLen := uint32(len(msgBytes))
	lenBuf[0] = byte(msgLen >> 24)
	lenBuf[1] = byte(msgLen >> 16)
	lenBuf[2] = byte(msgLen >> 8)
	lenBuf[3] = byte(msgLen)

	if _, err := c.conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("failed to write message length: %v", err)
	}
	if _, err := c.conn.Write(msgBytes); err != nil {
		return nil, fmt.Errorf("failed to write message: %v", err)
	}

	// Read response length
	if _, err := c.conn.Read(lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}
	respLen := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if respLen > MaxMessageSize {
		return nil, fmt.Errorf("response too large: %d bytes", respLen)
	}

	// Read response
	respBuf := make([]byte, respLen)
	if _, err := c.conn.Read(respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	var resp AgentResponse
	if err := json.Unmarshal(respBuf, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("agent error: %s", resp.Error)
	}

	return resp.Payload, nil
}

func (c *UnixSocketClient) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	conn, err := net.DialTimeout("unix", c.socketPath, c.timeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// CheckPodAdmission checks if a pod should be admitted
func (c *UnixSocketClient) CheckPodAdmission(req *PodAdmissionRequest) (*PodAdmissionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	respPayload, err := c.sendRequest(ctx, MsgTypePodAdmission, req)
	if err != nil {
		glog.Errorf("External agent pod admission check failed: %v", err)
		// On error, allow the pod (fail-open policy)
		return &PodAdmissionResponse{
			Allowed: true,
			Reason:  "AgentUnavailable",
			Message: fmt.Sprintf("External agent unavailable, allowing pod: %v", err),
		}, nil
	}

	var resp PodAdmissionResponse
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		glog.Errorf("Failed to unmarshal pod admission response: %v", err)
		return &PodAdmissionResponse{
			Allowed: true,
			Reason:  "ParseError",
			Message: fmt.Sprintf("Failed to parse agent response: %v", err),
		}, nil
	}

	return &resp, nil
}

// OnContainerLifecycle is called on container lifecycle events
func (c *UnixSocketClient) OnContainerLifecycle(req *ContainerLifecycleRequest) (*ContainerLifecycleResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	respPayload, err := c.sendRequest(ctx, MsgTypeContainerLifecycle, req)
	if err != nil {
		glog.Warningf("External agent container lifecycle hook failed: %v", err)
		// On error, don't block container operations
		return &ContainerLifecycleResponse{
			Success: true,
			Message: fmt.Sprintf("Agent unavailable: %v", err),
		}, nil
	}

	var resp ContainerLifecycleResponse
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		glog.Warningf("Failed to unmarshal container lifecycle response: %v", err)
		return &ContainerLifecycleResponse{
			Success: true,
			Message: fmt.Sprintf("Failed to parse response: %v", err),
		}, nil
	}

	return &resp, nil
}

// GetPodMeasurement requests measurements for a pod
func (c *UnixSocketClient) GetPodMeasurement(req *PodMeasurementRequest) (*PodMeasurementResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	respPayload, err := c.sendRequest(ctx, MsgTypePodMeasurement, req)
	if err != nil {
		return nil, fmt.Errorf("pod measurement request failed: %v", err)
	}

	var resp PodMeasurementResponse
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal measurement response: %v", err)
	}

	return &resp, nil
}

// Close closes the connection to the agent
func (c *UnixSocketClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
