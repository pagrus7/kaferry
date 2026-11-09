package compat

import (
	"fmt"
	"syscall"
	"unsafe"
)

// rssSample returns live resident memory in KiB, matching ps's RSS units.
func rssSample(pid int) (int64, error) {
	const (
		procInfoCallPIDInfo = 2
		procPIDTaskInfo     = 4
	)
	// Darwin's proc_taskinfo has six uint64_t fields and twelve int32_t
	// fields. Its second uint64_t, pti_resident_size, is measured in bytes.
	var info struct {
		_            uint64
		residentSize uint64
		_            [80]byte
	}
	size := unsafe.Sizeof(info)
	n, _, errno := syscall.Syscall6(syscall.SYS_PROC_INFO, procInfoCallPIDInfo,
		uintptr(pid), procPIDTaskInfo, 0, uintptr(unsafe.Pointer(&info)), size)
	if errno != 0 {
		return 0, fmt.Errorf("proc_pidinfo(%d, PROC_PIDTASKINFO): %w", pid, errno)
	}
	if n != size {
		return 0, fmt.Errorf("proc_pidinfo(%d, PROC_PIDTASKINFO): read %d bytes, want %d", pid, n, size)
	}
	if info.residentSize < 1024 {
		return 0, fmt.Errorf("proc_pidinfo(%d, PROC_PIDTASKINFO): no resident memory", pid)
	}
	return int64(info.residentSize / 1024), nil
}
