// Step Processing test framework with inter-process messaging support
//
// This framework allows running multiple Go test processes that can communicate
// with each other through a messaging system. The main coordinator process
// uses Unix domain sockets to communicate with child processes.
//
// Message Format:
// - Child processes send sync messages: LITEBASE_TEST_SYNC=[STEP_NAME]
// - Coordinator sends step messages: LITEBASE_TEST_STEP=[PROCESS_NAME]:[STEP_NAME]
//
// Usage:
// - Use StepProcess.Step(name) to broadcast a step completion
// - Use StepProcess.WaitForStep(name) to wait for a step from any process
package test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type StepProcessor struct {
	ctx       context.Context
	dataPath  string
	name      string
	signature string
	tests     map[string]*StepTest
	// Messaging system
	completedSteps map[string]bool // Buffer for completed steps
	messageQueue   chan Message
	stepMutex      sync.RWMutex
	stepWaiters    map[string]chan struct{}
	// Unix socket communication
	connections map[string]net.Conn
	connMutex   sync.RWMutex
	listener    net.Listener
	socketDir   string
	// Connection tracking
	allConnected      chan struct{}
	connectedOnce     sync.Once
	expectedProcesses map[string]bool
	// Message buffering
	childAllConnected  chan struct{}
	childConnectedOnce sync.Once
	messagesMutex      sync.Mutex
	pendingMessages    []Message
}

type Message struct {
	ProcessName string
	StepName    string
}

type StepTest struct {
	cmd        *exec.Cmd
	function   func(sp *StepProcess)
	process    *StepProcess
	socketPath string
}

// Create a new distributed test
func WithSteps(t *testing.T, fn func(sp *StepProcessor)) {
	// Check if this process has already completed (process-local check)
	if os.Getenv("LITEBASE_TEST_COMPLETED") == "1" {
		fmt.Printf("[CHILD] Process already completed, ignoring re-entry\n")
		return
	}

	// Wrap the test context
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sp := &StepProcessor{
		ctx:               ctx,
		name:              t.Name(),
		tests:             make(map[string]*StepTest),
		messageQueue:      make(chan Message, 100),
		stepWaiters:       make(map[string]chan struct{}),
		completedSteps:    make(map[string]bool),
		connections:       make(map[string]net.Conn),
		expectedProcesses: make(map[string]bool),
		allConnected:      make(chan struct{}),
		childAllConnected: make(chan struct{}),
	}

	defer sp.Cleanup()

	processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")

	// If we are not running a specific process, setup and run all of them
	if processName == "" {
		// Allow the caller to populate tests first
		fn(sp)

		sp.dataPath = fmt.Sprintf("./../../.test/%s", CreateHash(32))
		sp.signature = CreateHash(64)

		// Create socket directory
		sp.socketDir = filepath.Join(sp.dataPath, "sockets")
		os.MkdirAll(sp.socketDir, 0755)

		// Setup Unix socket listener
		sp.setupSocketListener()
		sp.setupProcesses()

		sp.Start(t)

		// Cleanup
		t.Cleanup(func() {
			os.RemoveAll(sp.dataPath)
		})
	} else {
		// Running as a child process - connect to coordinator first
		processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")

		if processName != "" {
			sp.connectToCoordinator(processName)
		}

		// Populate tests first, then start message handling
		fn(sp)

		// Start message handling in background
		go sp.startMessageBroker()

		// Child processes should wait for all connections before proceeding
		sp.waitForAllProcessesToConnect()

		if test, exists := sp.tests[processName]; exists {
			// var testFailed bool
			t.Run(processName, func(t *testing.T) {
				test.function(test.process)
			})

			// Mark this process as completed to prevent re-entry
			os.Setenv("LITEBASE_TEST_COMPLETED", "1")

			// Clean up child process resources
			sp.Cleanup()

			// Return from WithSteps function to terminate child process cleanly
			return
		}
	}
}

// Run a distributed test process
func (sp *StepProcessor) Run(name string, fn func(s *StepProcess)) *StepProcess {
	sp.tests[name] = &StepTest{
		function: fn,
		process: &StepProcess{
			sp:               sp,
			processName:      name,
			expectedExitCode: 0,
		},
	}

	return sp.tests[name].process
}

// Setup Unix socket listener for coordinator
func (sp *StepProcessor) setupSocketListener() {
	socketPath := filepath.Join(sp.socketDir, "coordinator.sock")

	// Remove existing socket file if it exists
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)

	if err != nil {
		panic(fmt.Sprintf("Failed to create Unix socket listener: %v", err))
	}

	sp.listener = listener

	// Start accepting connections in background
	go sp.acceptConnections()
}

// Accept connections from child processes
func (sp *StepProcessor) acceptConnections() {
	for {
		select {
		case <-sp.ctx.Done():
			return
		default:
			conn, err := sp.listener.Accept()
			if err != nil {
				select {
				case <-sp.ctx.Done():
					return // Context cancelled, normal shutdown
				default:
					// Check if the error is due to a closed network connection
					if strings.Contains(err.Error(), "use of closed network connection") {
						// Listener was closed during cleanup, exit gracefully
						return
					}

					fmt.Printf("Failed to accept connection: %v\n", err)
					continue
				}
			}

			go sp.handleConnection(conn)
		}
	}
}

// Handle connection from a child process
func (sp *StepProcessor) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var processName string

	// First message should be the process name
	if scanner.Scan() {
		line := scanner.Text()

		if after, ok := strings.CutPrefix(line, "PROCESS_NAME="); ok {
			processName = after

			sp.connMutex.Lock()
			sp.connections[processName] = conn

			// Check if all expected processes are now connected
			allConnected := len(sp.connections) == len(sp.expectedProcesses)

			for expectedProcess := range sp.expectedProcesses {
				if _, connected := sp.connections[expectedProcess]; !connected {
					allConnected = false
					break
				}
			}

			sp.connMutex.Unlock()

			// Signal that all processes are connected
			if allConnected {
				sp.connectedOnce.Do(func() {
					close(sp.allConnected)
					// Send "all connected" signal to all child processes
					sp.sendAllConnectedSignal()
					// Process any pending messages that were buffered
					sp.processPendingMessages()
				})
			}
		}
	}

	// Handle messages from this process
	for scanner.Scan() {
		select {
		case <-sp.ctx.Done():
			fmt.Printf("[COORDINATOR] Context cancelled, stopping handler for %s\n", processName)
			return
		default:
		}

		line := scanner.Text()

		// Check if this line contains a sync message
		if after, ok := strings.CutPrefix(line, "LITEBASE_TEST_SYNC="); ok {
			stepName := after

			msg := Message{
				ProcessName: processName,
				StepName:    stepName,
			}

			// Check if all processes are connected
			select {
			case <-sp.allConnected:
				// All processes connected, process message immediately
				sp.processStepMessage(msg)
			default:
				// Not all processes connected yet, buffer the message
				sp.messagesMutex.Lock()
				sp.pendingMessages = append(sp.pendingMessages, msg)
				sp.messagesMutex.Unlock()
			}
		}
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		fmt.Printf("[COORDINATOR] Scanner error for %s: %v\n", processName, err)
	}

	// Remove connection when done
	sp.connMutex.Lock()
	delete(sp.connections, processName)
	sp.connMutex.Unlock()
}

// Setup all of the processes to be run
func (sp *StepProcessor) setupProcesses() {
	// Track expected processes
	for name := range sp.tests {
		sp.expectedProcesses[name] = true
	}

	for name, test := range sp.tests {
		cmd := exec.Command("go", "test", "-run", sp.name)

		cmd.Env = append(os.Environ(),
			fmt.Sprintf("LITEBASE_DISTRIBUTED_TEST_RUN=%s", name),
			fmt.Sprintf("LITEBASE_TEST_DATA_PATH=%s", sp.dataPath),
			fmt.Sprintf("LITEBASE_TEST_SIGNATURE=%s", sp.signature),
			fmt.Sprintf("LITEBASE_SOCKET_DIR=%s", sp.socketDir))

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		test.cmd = cmd
		test.socketPath = filepath.Join(sp.socketDir, "coordinator.sock")
	}
}

// Start all of the processes and wait for them to complete
func (sp *StepProcessor) Start(t *testing.T) {
	// Start the message broker
	sp.startMessageBroker()

	// Give the socket listener a moment to be ready
	time.Sleep(100 * time.Millisecond)

	wg := sync.WaitGroup{}
	wg.Add(len(sp.tests))

	// Start all processes
	for tname, tt := range sp.tests {
		go func(testName string, test *StepTest) {
			defer wg.Done()

			err := test.cmd.Start()
			if err != nil {
				t.Errorf("Failed to start process %s: %v", testName, err)
				return
			}

			err = test.cmd.Wait()

			if exitErr, ok := err.(*exec.ExitError); ok {
				if exitErr.ExitCode() != test.process.expectedExitCode {
					t.Errorf(
						"Distributed test %s should have exited with code %d, got %d",
						testName,
						test.process.expectedExitCode,
						exitErr.ExitCode(),
					)
				}
			}
		}(tname, tt)
	}

	// Wait for all processes to connect before allowing them to proceed
	select {
	case <-sp.allConnected:
	case <-time.After(2 * time.Second): // Keep under 3s test timeout
		t.Errorf("Timeout waiting for all processes to connect")
		return
	case <-sp.ctx.Done():
		return
	}

	wg.Wait()

	// All processes have completed, signal shutdown and cleanup
	sp.Cleanup()

	// Add a small delay to ensure resources are fully released before next test run
	time.Sleep(200 * time.Millisecond)
}

// Send a step message to all other processes via Unix sockets
func (sp *StepProcessor) sendStepToProcesses(processName, stepName string) {
	message := fmt.Sprintf("LITEBASE_TEST_STEP=%s:%s\n", processName, stepName)

	sp.connMutex.RLock()
	connections := make(map[string]net.Conn)

	for name, conn := range sp.connections {
		if name != processName {
			connections[name] = conn
		}
	}

	sp.connMutex.RUnlock()

	for name, conn := range connections {
		_, err := conn.Write([]byte(message))

		if err != nil {
			fmt.Printf("[COORDINATOR] Error writing to %s socket: %v\n", name, err)

			// Remove failed connection
			sp.connMutex.Lock()
			delete(sp.connections, name)
			sp.connMutex.Unlock()
		}
	}
}

// Send "all connected" signal to all child processes
func (sp *StepProcessor) sendAllConnectedSignal() {
	message := "LITEBASE_TEST_ALL_CONNECTED\n"

	sp.connMutex.RLock()
	connections := make(map[string]net.Conn)
	for name, conn := range sp.connections {
		connections[name] = conn
	}
	sp.connMutex.RUnlock()

	for name, conn := range connections {
		_, err := conn.Write([]byte(message))

		if err != nil {
			fmt.Printf("[COORDINATOR] Error sending all connected signal to %s: %v\n", name, err)
		}
	}
}

// Wait for a specific step to be completed
func (sp *StepProcessor) waitForStep(stepName string) {
	sp.stepMutex.Lock()

	// Check if step was already completed
	if sp.completedSteps[stepName] {
		fmt.Printf("[BROKER] Step %s already completed\n", stepName)
		sp.stepMutex.Unlock()
		return
	}

	// Create waiter if it doesn't exist
	if _, exists := sp.stepWaiters[stepName]; !exists {
		sp.stepWaiters[stepName] = make(chan struct{})
	}
	waiter := sp.stepWaiters[stepName]
	sp.stepMutex.Unlock()

	select {
	case <-sp.ctx.Done():
		return
	case <-waiter:
		return
	}
}

// Connect to coordinator's Unix socket as a child process
func (sp *StepProcessor) connectToCoordinator(processName string) {
	// Connect to coordinator's Unix socket
	socketPath := os.Getenv("LITEBASE_SOCKET_DIR")

	if socketPath == "" {
		fmt.Printf("[CHILD] No socket directory specified\n")
		return
	}

	coordSocketPath := filepath.Join(socketPath, "coordinator.sock")

	// Retry connection with backoff
	var conn net.Conn
	var err error

	for attempt := range 10 {
		conn, err = net.Dial("unix", coordSocketPath)
		if err == nil {
			break
		}

		fmt.Printf("[CHILD] Connection attempt %d failed: %v, retrying...\n", attempt+1, err)
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}

	if err != nil {
		fmt.Printf("[CHILD] Failed to connect to coordinator socket after retries: %v\n", err)
		return
	}

	// Send process name as first message
	_, err = conn.Write(fmt.Appendf(nil, "PROCESS_NAME=%s\n", processName))

	if err != nil {
		fmt.Printf("[CHILD] Failed to send process name: %v\n", err)
		conn.Close()
		return
	}

	// Store connection for sending messages
	sp.connMutex.Lock()
	sp.connections[processName] = conn
	sp.connMutex.Unlock()

	// Start handling messages in background
	go func() {
		defer func() {
			sp.connMutex.Lock()
			delete(sp.connections, processName)
			sp.connMutex.Unlock()
			conn.Close()
		}()

		scanner := bufio.NewScanner(conn)

		for scanner.Scan() {
			select {
			case <-sp.ctx.Done():
				fmt.Printf("[CHILD %s] Context cancelled, stopping message handler\n", processName)
				return
			default:
			}

			line := scanner.Text()

			// Check if this is the "all connected" signal
			if strings.HasPrefix(line, "LITEBASE_TEST_ALL_CONNECTED") {
				// Signal that all processes are connected for this child
				sp.childConnectedOnce.Do(func() {
					close(sp.childAllConnected)
				})
				continue
			}

			// Check if this line contains a step message from another process
			if after, ok := strings.CutPrefix(line, "LITEBASE_TEST_STEP="); ok {
				stepInfo := after

				parts := strings.SplitN(stepInfo, ":", 2)

				if len(parts) == 2 {
					stepName := parts[1]

					select {
					case sp.messageQueue <- Message{
						ProcessName: parts[0],
						StepName:    stepName,
					}:
					case <-sp.ctx.Done():
						return
					}
				}
			}
		}

		if err := scanner.Err(); err != nil {
			// fmt.Printf("[CHILD %s] Scanner error: %v\n", processName, err)
		}
	}()
}

// Process a step message by sending it to other processes and the message broker
func (sp *StepProcessor) processStepMessage(msg Message) {
	// Send this step message to all other processes
	sp.sendStepToProcesses(msg.ProcessName, msg.StepName)

	// Also process it locally for the coordinator's message broker
	select {
	case sp.messageQueue <- msg:
	case <-sp.ctx.Done():
		return
	}
}

// Process all pending messages when all connections are established
func (sp *StepProcessor) processPendingMessages() {
	sp.messagesMutex.Lock()
	defer sp.messagesMutex.Unlock()

	if len(sp.pendingMessages) > 0 {
		fmt.Printf("[COORDINATOR] Processing %d pending messages\n", len(sp.pendingMessages))

		for _, msg := range sp.pendingMessages {
			fmt.Printf("[COORDINATOR] Processing pending message: %s from %s\n", msg.StepName, msg.ProcessName)
			sp.processStepMessage(msg)
		}

		// Clear pending messages
		sp.pendingMessages = nil
	}
}

// Start the message broker to process messages from the queue
func (sp *StepProcessor) startMessageBroker() {
	go func() {
		for {
			select {
			case <-sp.ctx.Done():
				return
			case msg := <-sp.messageQueue:
				sp.stepMutex.Lock()

				// Mark step as completed
				sp.completedSteps[msg.StepName] = true

				// Notify any waiters for this step
				if waiter, exists := sp.stepWaiters[msg.StepName]; exists {
					close(waiter)
					delete(sp.stepWaiters, msg.StepName)
				}

				sp.stepMutex.Unlock()
			}
		}
	}()
}

// Cleanup resources and stop all processes
func (sp *StepProcessor) Cleanup() {
	// Close Unix socket listener
	if sp.listener != nil {
		sp.listener.Close()
	}

	// Close all connections
	sp.connMutex.Lock()

	for _, conn := range sp.connections {
		conn.Close()
	}

	sp.connections = make(map[string]net.Conn)
	sp.connMutex.Unlock()

	// Kill any running processes
	for _, test := range sp.tests {
		if test.cmd != nil && test.cmd.Process != nil {
			test.cmd.Process.Kill()
		}
	}

	// Clean up socket files
	if sp.socketDir != "" {
		os.RemoveAll(sp.socketDir)
	}
}

// Wait for all processes to connect (for child processes)
func (sp *StepProcessor) waitForAllProcessesToConnect() {
	// Child processes wait for a signal from coordinator that all are connected
	processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")
	if processName == "" {
		return // Not a child process
	}

	// Wait for the coordinator to send an "all connected" signal
	// This will be received via the socket connection as a special message

	// The childAllConnected channel will be closed when we receive the signal
	select {
	case <-sp.childAllConnected:
	case <-time.After(10 * time.Second):
		fmt.Printf("[CHILD %s] Timeout waiting for all processes to connect\n", processName)
	case <-sp.ctx.Done():
		return
	}
}
