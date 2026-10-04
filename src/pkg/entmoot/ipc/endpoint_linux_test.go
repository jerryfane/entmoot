//go:build linux

package ipc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// denyUnixSocketsOnThisThread makes socket(AF_UNIX, ...) on the calling OS
// thread fail with errno, as seccomp-confined cloud runtimes do. The caller
// locks its goroutine to the thread and exits without unlocking it, so the
// runtime discards the filtered thread instead of reusing it.
func denyUnixSocketsOnThisThread(errno unix.Errno) error {
	var arch uint32
	switch runtime.GOARCH {
	case "amd64":
		arch = unix.AUDIT_ARCH_X86_64
	case "arm64":
		arch = unix.AUDIT_ARCH_AARCH64
	default:
		return fmt.Errorf("no seccomp audit arch for %s", runtime.GOARCH)
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4}, // seccomp_data.arch
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 5, K: arch},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}, // seccomp_data.nr
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 3, K: unix.SYS_SOCKET},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16}, // args[0], little endian
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 1, K: unix.AF_UNIX},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(errno)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	return unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&prog)), 0, 0)
}

func listenWithoutUnixSockets(t *testing.T, path, transport string, errno unix.Errno) (*Listener, error) {
	t.Helper()
	type result struct {
		l        *Listener
		err      error
		setupErr error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		if err := denyUnixSocketsOnThisThread(errno); err != nil {
			done <- result{setupErr: err}
			return
		}
		l, err := Listen(path, transport)
		done <- result{l: l, err: err}
	}()
	r := <-done
	if r.setupErr != nil {
		t.Fatalf("deny Unix sockets: %v", r.setupErr)
	}
	return r.l, r.err
}

// A runtime that refuses Unix sockets gets authenticated loopback control from
// the default "auto" transport, while "unix" stays strict and failures that
// are not a refusal stay fatal.
func TestListenAutoServesTCPWhenUnixSocketsForbidden(t *testing.T) {
	for _, errno := range []unix.Errno{unix.EPERM, unix.EACCES, unix.EAFNOSUPPORT} {
		t.Run(unix.ErrnoName(errno), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "control.sock")
			if l, err := listenWithoutUnixSockets(t, path, "unix", errno); !errors.Is(err, errno) {
				if l != nil {
					l.Close()
				}
				t.Fatalf("strict unix transport: got %v, want %v", err, errno)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("strict unix failure left an endpoint: %v", err)
			}

			l, err := listenWithoutUnixSockets(t, path, "auto", errno)
			if err != nil {
				t.Fatalf("auto transport: %v", err)
			}
			info, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
				t.Fatalf("fallback endpoint mode %v, want private regular file", info.Mode())
			}
			stale, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			dispatch := serveControlFixture(t, l)
			requestControl(t, path)
			if dispatch.Load() != 1 {
				t.Fatal("authenticated request did not reach application")
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			// A killed daemon leaves its endpoint file behind; restart replaces it.
			if err := os.WriteFile(path, stale, 0600); err != nil {
				t.Fatal(err)
			}
			next, err := listenWithoutUnixSockets(t, path, "auto", errno)
			if err != nil {
				t.Fatalf("auto restart over stale endpoint: %v", err)
			}
			nextDispatch := serveControlFixture(t, next)
			requestControl(t, path)
			if nextDispatch.Load() != 1 {
				t.Fatal("restarted endpoint did not accept authenticated request")
			}
		})
	}

	t.Run("unix allowed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "control.sock")
		l, err := Listen(path, "auto")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("auto did not keep the Unix socket where allowed: %v %v", info, err)
		}
	})

	t.Run("unrelated unix error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, strings.Repeat("d", 120), "control.sock")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if l, err := Listen(path, "auto"); err == nil {
			l.Close()
			t.Fatal("auto hid an over-long socket path behind tcp")
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed listen left an endpoint: %v", err)
		}
	})
}

// The TCP endpoint chosen by the "auto" fallback is the one a restricted cloud
// gets without flags, so it must evict stalled clients like explicit "tcp".
func TestListenAutoFallbackEvictsStalledClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	l, err := listenWithoutUnixSockets(t, path, "auto", unix.EPERM)
	if err != nil {
		t.Fatalf("auto transport: %v", err)
	}
	if l.UnixDenied() == nil {
		t.Fatal("auto transport did not fall back to tcp")
	}
	assertStalledClientsEvictedOldestFirst(t, l, path)
}
