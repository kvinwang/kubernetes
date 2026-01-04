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

// Package example provides a reference implementation of an external agent
// that can be used for testing and as a starting point for custom agents.
package example

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"k8s.io/api/core/v1"
)

// ExampleAgent is a reference implementation of an external kubelet agent
type ExampleAgent struct {
	socketPath string
	listener   net.Listener
	mu         sync.Mutex
	running    bool

	// Callbacks for customization
	OnPodAdmission        func(pod *v1.Pod, otherPods []*v1.Pod) (allowed bool, reason, message string)
	OnContainerLifecycle  func(pod *v1.Pod, container *v1.Container, containerID, event string) (success bool, message string, measurements map[string]string)
	OnPodMeasurement      func(pod *v1.Pod, containerID string) (measurements map[string]string, err error)
}

// NewExampleAgent creates a new example agent
func NewExampleAgent(socketPath string) *ExampleAgent {
	return &ExampleAgent{
		socketPath: socketPath,
		// Default implementations
		OnPodAdmission: func(pod *v1.Pod, otherPods []*v1.Pod) (bool, string, string) {
			// Default: allow all pods
			log.Printf("Pod admission check for %s/%s - ALLOWED", pod.Namespace, pod.Name)
			return true, "", ""
		},
		OnContainerLifecycle: func(pod *v1.Pod, container *v1.Container, containerID, event string) (bool, string, map[string]string) {
			// Default: log and succeed
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
				"event":     event,
			}
		},
		OnPodMeasurement: func(pod *v1.Pod, containerID string) (map[string]string, error) {
			// Default: return basic measurements
			return map[string]string{
				"timestamp": time.Now().Format(time.RFC3339),
				"pod":       pod.Namespace + "/" + pod.Name,
			}, nil
		},
	}
}

// Message types (must match client)
const (
	MsgTypePodAdmission       = "pod_admission"
	MsgTypeContainerLifecycle = "container_lifecycle"
	MsgTypePodMeasurement     = "pod_measurement"
)

// AgentMessage is the wire format for incoming messages
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

// PodAdmissionRequest matches the client's request format
type PodAdmissionRequest struct {
	Pod       *v1.Pod   `json:"pod"`
	OtherPods []*v1.Pod `json:"otherPods"`
}

// PodAdmissionResponse matches the client's response format
type PodAdmissionResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerLifecycleRequest matches the client's request format
type ContainerLifecycleRequest struct {
	Pod         *v1.Pod       `json:"pod"`
	Container   *v1.Container `json:"container"`
	ContainerID string        `json:"containerID"`
	Event       string        `json:"event"`
}

// ContainerLifecycleResponse matches the client's response format
type ContainerLifecycleResponse struct {
	Success      bool              `json:"success"`
	Message      string            `json:"message,omitempty"`
	Measurements map[string]string `json:"measurements,omitempty"`
}

// PodMeasurementRequest matches the client's request format
type PodMeasurementRequest struct {
	Pod         *v1.Pod `json:"pod"`
	ContainerID string  `json:"containerID,omitempty"`
}

// PodMeasurementResponse matches the client's response format
type PodMeasurementResponse struct {
	Success      bool              `json:"success"`
	Measurements map[string]string `json:"measurements,omitempty"`
	Message      string            `json:"message,omitempty"`
}

// Start starts the agent server
func (a *ExampleAgent) Start() error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("agent already running")
	}

	// Remove existing socket file if present
	os.Remove(a.socketPath)

	listener, err := net.Listen("unix", a.socketPath)
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %v", a.socketPath, err)
	}

	a.listener = listener
	a.running = true
	a.mu.Unlock()

	log.Printf("External agent listening on %s", a.socketPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			a.mu.Lock()
			if !a.running {
				a.mu.Unlock()
				return nil // Stopped gracefully
			}
			a.mu.Unlock()
			log.Printf("Accept error: %v", err)
			continue
		}

		go a.handleConnection(conn)
	}
}

// Stop stops the agent server
func (a *ExampleAgent) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}

	a.running = false
	if a.listener != nil {
		a.listener.Close()
	}
	os.Remove(a.socketPath)
	return nil
}

func (a *ExampleAgent) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read message length (4 bytes, big endian)
		lenBuf := make([]byte, 4)
		if _, err := conn.Read(lenBuf); err != nil {
			return // Connection closed
		}
		msgLen := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])

		// Read message
		msgBuf := make([]byte, msgLen)
		if _, err := conn.Read(msgBuf); err != nil {
			log.Printf("Read error: %v", err)
			return
		}

		var msg AgentMessage
		if err := json.Unmarshal(msgBuf, &msg); err != nil {
			log.Printf("Unmarshal error: %v", err)
			a.sendError(conn, "invalid message format")
			continue
		}

		// Handle message based on type
		var respPayload interface{}
		var respErr error

		switch msg.Type {
		case MsgTypePodAdmission:
			respPayload, respErr = a.handlePodAdmission(msg.Payload)
		case MsgTypeContainerLifecycle:
			respPayload, respErr = a.handleContainerLifecycle(msg.Payload)
		case MsgTypePodMeasurement:
			respPayload, respErr = a.handlePodMeasurement(msg.Payload)
		default:
			respErr = fmt.Errorf("unknown message type: %s", msg.Type)
		}

		if respErr != nil {
			a.sendError(conn, respErr.Error())
			continue
		}

		a.sendResponse(conn, respPayload)
	}
}

func (a *ExampleAgent) handlePodAdmission(payload json.RawMessage) (*PodAdmissionResponse, error) {
	var req PodAdmissionRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid pod admission request: %v", err)
	}

	allowed, reason, message := a.OnPodAdmission(req.Pod, req.OtherPods)
	return &PodAdmissionResponse{
		Allowed: allowed,
		Reason:  reason,
		Message: message,
	}, nil
}

func (a *ExampleAgent) handleContainerLifecycle(payload json.RawMessage) (*ContainerLifecycleResponse, error) {
	var req ContainerLifecycleRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid container lifecycle request: %v", err)
	}

	success, message, measurements := a.OnContainerLifecycle(req.Pod, req.Container, req.ContainerID, req.Event)
	return &ContainerLifecycleResponse{
		Success:      success,
		Message:      message,
		Measurements: measurements,
	}, nil
}

func (a *ExampleAgent) handlePodMeasurement(payload json.RawMessage) (*PodMeasurementResponse, error) {
	var req PodMeasurementRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid pod measurement request: %v", err)
	}

	measurements, err := a.OnPodMeasurement(req.Pod, req.ContainerID)
	if err != nil {
		return &PodMeasurementResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &PodMeasurementResponse{
		Success:      true,
		Measurements: measurements,
	}, nil
}

func (a *ExampleAgent) sendError(conn net.Conn, errMsg string) {
	resp := AgentResponse{
		Success: false,
		Error:   errMsg,
	}
	a.writeResponse(conn, resp)
}

func (a *ExampleAgent) sendResponse(conn net.Conn, payload interface{}) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		a.sendError(conn, fmt.Sprintf("failed to marshal response: %v", err))
		return
	}

	resp := AgentResponse{
		Success: true,
		Payload: payloadBytes,
	}
	a.writeResponse(conn, resp)
}

func (a *ExampleAgent) writeResponse(conn net.Conn, resp AgentResponse) {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	// Write length prefix
	lenBuf := make([]byte, 4)
	respLen := uint32(len(respBytes))
	lenBuf[0] = byte(respLen >> 24)
	lenBuf[1] = byte(respLen >> 16)
	lenBuf[2] = byte(respLen >> 8)
	lenBuf[3] = byte(respLen)

	conn.Write(lenBuf)
	conn.Write(respBytes)
}
