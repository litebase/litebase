//go:build windows

package test

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

var (
	kernel32                     = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObject          = kernel32.NewProc("CreateJobObjectW")
	procAssignProcessToJob       = kernel32.NewProc("AssignProcessToJobObject")
	procSuspendThread            = kernel32.NewProc("SuspendThread")
	procResumeThread             = kernel32.NewProc("ResumeThread")
	procOpenThread               = kernel32.NewProc("OpenThread")
	procCloseHandle              = kernel32.NewProc("CloseHandle")
	procCreateToolhelp32Snapshot = kernel32.NewProc("CreateToolhelp32Snapshot")
	procThread32First            = kernel32.NewProc("Thread32First")
	procThread32Next             = kernel32.NewProc("Thread32Next")
)

const (
	THREAD_SUSPEND_RESUME = 0x0002
	TH32CS_SNAPTHREAD     = 0x00000004
)

type ThreadEntry32 struct {
	dwSize             uint32
	cntUsage           uint32
	th32ThreadID       uint32
	th32OwnerProcessID uint32
	tpBasePri          int32
	tpDeltaPri         int32
	dwFlags            uint32
}

// Map to track suspended threads for each process
var suspendedThreads = make(map[uint32][]syscall.Handle)
var suspendMutex sync.Mutex

// pauseProcess pauses a process on Windows by suspending all its threads
func pauseProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return errors.New("process not started")
	}

	pid := uint32(cmd.Process.Pid)

	suspendMutex.Lock()
	defer suspendMutex.Unlock()

	// Check if already suspended
	if _, exists := suspendedThreads[pid]; exists {
		return nil // Already suspended
	}

	threadIDs, err := getProcessThreadIDs(pid)
	if err != nil {
		return fmt.Errorf("failed to get thread IDs: %v", err)
	}

	var handles []syscall.Handle
	for _, threadID := range threadIDs {
		handle, _, _ := procOpenThread.Call(uintptr(THREAD_SUSPEND_RESUME), 0, uintptr(threadID))
		if handle != 0 {
			procSuspendThread.Call(handle)
			handles = append(handles, syscall.Handle(handle))
		}
	}

	suspendedThreads[pid] = handles
	return nil
}

// resumeProcess resumes a process on Windows by resuming all its suspended threads
func resumeProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return errors.New("process not started")
	}

	pid := uint32(cmd.Process.Pid)

	suspendMutex.Lock()
	defer suspendMutex.Unlock()

	handles, exists := suspendedThreads[pid]
	if !exists {
		return nil // Not suspended
	}

	for _, handle := range handles {
		procResumeThread.Call(uintptr(handle))
		procCloseHandle.Call(uintptr(handle))
	}

	delete(suspendedThreads, pid)
	return nil
}

// getProcessThreadIDs returns all thread IDs for a given process ID
func getProcessThreadIDs(processID uint32) ([]uint32, error) {
	snapshot, _, _ := procCreateToolhelp32Snapshot.Call(TH32CS_SNAPTHREAD, 0)
	if snapshot == uintptr(syscall.InvalidHandle) {
		return nil, errors.New("failed to create thread snapshot")
	}
	defer procCloseHandle.Call(snapshot)

	var threadIDs []uint32
	var te ThreadEntry32
	te.dwSize = uint32(unsafe.Sizeof(te))

	ret, _, _ := procThread32First.Call(snapshot, uintptr(unsafe.Pointer(&te)))
	if ret == 0 {
		return nil, errors.New("failed to get first thread")
	}

	for {
		if te.th32OwnerProcessID == processID {
			threadIDs = append(threadIDs, te.th32ThreadID)
		}

		ret, _, _ := procThread32Next.Call(snapshot, uintptr(unsafe.Pointer(&te)))
		if ret == 0 {
			break
		}
	}

	return threadIDs, nil
}
