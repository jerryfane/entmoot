package ipc

import (
	"container/list"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const endpointVersion = "entmoot.control.tls.v1"
const authenticationTimeout = 5 * time.Second

// maxPendingAuthentications bounds TCP control connections that have not yet
// presented the credential. At capacity the oldest one is closed, so local
// clients that connect and stall cannot lock out a client that authenticates
// promptly; a loopback attacker must instead open connections faster than one
// local TLS handshake completes. This limits, not eliminates, local DoS.
const maxPendingAuthentications = 256

// ErrControlActive means another daemon owns this data root's control endpoint.
var ErrControlActive = errors.New("control endpoint is already owned by a daemon")

// A TCP endpoint occupies the same rendezvous path as a Unix socket. Its token
// is a local credential, never an Entmoot key or a remotely usable ESP token.
type controlEndpoint struct {
	Version     string `json:"version"`
	Address     string `json:"address"`
	Certificate string `json:"certificate"`
	Token       string `json:"token"`
}

// Listener owns both a control endpoint and its exclusive file lease.
type Listener struct {
	net.Listener
	lock       *os.File
	path       string
	file       os.FileInfo
	once       sync.Once
	closeErr   error
	stopOnce   sync.Once
	stopErr    error
	tlsConfig  *tls.Config
	token      [32]byte
	pendingMu  sync.Mutex
	pending    list.List // raw connections awaiting authentication, oldest first
	unixDenied error
}

// Listen creates the control endpoint for transport "unix", "tcp" or "auto"
// (the empty string means "unix"). "unix" is strictly a Unix socket; "tcp" is a
// mutually authenticated loopback TLS endpoint; "auto" creates the Unix socket
// unless the runtime forbids creating one, then serves the authenticated TCP
// endpoint instead. The file lease spans startup, service and cleanup, so a
// slow or stalled daemon cannot lose its endpoint to a competing process. The
// lock file is never unlinked (avoiding inode races).
func Listen(path, transport string) (*Listener, error) {
	if transport == "" {
		transport = "unix"
	}
	if transport != "unix" && transport != "tcp" && transport != "auto" {
		return nil, errors.New("control transport must be auto, unix or tcp")
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err = checkPrivateFile(lock); err != nil {
		lock.Close()
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrControlActive
		}
		return nil, err
	}
	l := &Listener{lock: lock, path: path}
	ok := false
	defer func() {
		if !ok {
			_ = l.Close()
		}
	}()
	// Compatibility with an already running Unix daemon that predates this lease.
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			if _, err := readEndpoint(path); err != nil {
				return nil, fmt.Errorf("refuse invalid control endpoint: %w", err)
			}
		}
		c, err := DialTimeout(path, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil, ErrControlActive
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if transport == "tcp" {
		err = l.listenTCP()
	} else {
		err = l.listenUnix(transport == "auto")
	}
	if err != nil {
		return nil, err
	}
	ok = true
	return l, nil
}

// UnixDenied returns the Unix socket creation error that made an "auto"
// listener serve authenticated loopback TCP instead, or nil.
func (l *Listener) UnixDenied() error { return l.unixDenied }

// unixSocketForbidden matches runtimes that refuse Unix sockets outright:
// seccomp or LSM denial (EPERM, EACCES) and a disabled address family
// (EAFNOSUPPORT, e.g. systemd RestrictAddressFamilies). Other failures, such
// as an over-long path, stay fatal so "auto" never hides a misconfiguration.
func unixSocketForbidden(err error) bool {
	return errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAFNOSUPPORT)
}

func (l *Listener) listenUnix(fallbackToTCP bool) error {
	ln, err := net.Listen("unix", l.path)
	if err != nil {
		if fallbackToTCP && unixSocketForbidden(err) {
			l.unixDenied = err
			if err := l.listenTCP(); err != nil {
				return fmt.Errorf("unix control socket forbidden (%v); authenticated loopback tcp control: %w", l.unixDenied, err)
			}
			return nil
		}
		return err
	}
	l.Listener = ln
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if l.file, err = os.Lstat(l.path); err != nil {
		return err
	}
	return os.Chmod(l.path, 0600)
}

func (l *Listener) listenTCP() error {
	cert, certPEM, err := newControlCertificate()
	if err != nil {
		return err
	}
	if _, err = rand.Read(l.token[:]); err != nil {
		return err
	}
	l.tlsConfig = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
	l.Listener, err = net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	endpoint := controlEndpoint{Version: endpointVersion, Address: l.Addr().String(), Certificate: string(certPEM), Token: base64.StdEncoding.EncodeToString(l.token[:])}
	tmp, err := os.CreateTemp(filepath.Dir(l.path), ".control-endpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	err = json.NewEncoder(tmp).Encode(endpoint)
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// Rename also replaces any socket file left by a failed Unix listen.
	if err = os.Rename(tmp.Name(), l.path); err != nil {
		return err
	}
	l.file, err = os.Lstat(l.path)
	return err
}

func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.tlsConfig == nil {
		return c, nil
	}
	l.pendingMu.Lock()
	var evicted net.Conn
	if l.pending.Len() >= maxPendingAuthentications {
		evicted = l.pending.Remove(l.pending.Front()).(net.Conn)
	}
	e := l.pending.PushBack(c)
	l.pendingMu.Unlock()
	if evicted != nil {
		// Its handshake or credential read fails and its handler returns.
		evicted.Close()
	}
	return &authenticatedConn{Conn: tls.Server(c, l.tlsConfig), token: l.token, release: func() {
		l.pendingMu.Lock()
		l.pending.Remove(e) // no-op once evicted or already released
		l.pendingMu.Unlock()
	}}, nil
}

// StopAccepting stops new connections without releasing endpoint ownership.
// Daemon shutdown calls Close only after its group writers and stores close.
func (l *Listener) StopAccepting() error {
	l.stopOnce.Do(func() {
		if l.Listener != nil {
			l.stopErr = l.Listener.Close()
		}
	})
	return l.stopErr
}

func (l *Listener) Close() error {
	l.once.Do(func() {
		l.closeErr = l.StopAccepting()
		if l.file != nil {
			if current, err := os.Lstat(l.path); err == nil && os.SameFile(l.file, current) {
				l.closeErr = errors.Join(l.closeErr, os.Remove(l.path))
			}
		}
		l.closeErr = errors.Join(l.closeErr, l.lock.Close())
	})
	return l.closeErr
}

type authenticatedConn struct {
	*tls.Conn
	token         [32]byte
	once          sync.Once
	release       func()
	err           error
	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *authenticatedConn) authenticate() error {
	c.once.Do(func() {
		defer c.release()
		c.deadlineMu.Lock()
		deadline := time.Now().Add(authenticationTimeout)
		if !c.readDeadline.IsZero() && c.readDeadline.Before(deadline) {
			deadline = c.readDeadline
		}
		_ = c.Conn.SetDeadline(deadline)
		c.deadlineMu.Unlock()
		defer func() {
			c.deadlineMu.Lock()
			defer c.deadlineMu.Unlock()
			_ = c.Conn.SetReadDeadline(c.readDeadline)
			_ = c.Conn.SetWriteDeadline(c.writeDeadline)
		}()
		var token [32]byte
		if _, err := io.ReadFull(c.Conn, token[:]); err != nil {
			c.err = errors.New("control authentication failed")
			return
		}
		if subtle.ConstantTimeCompare(token[:], c.token[:]) != 1 {
			c.err = errors.New("control authentication failed")
			return
		}
		_, c.err = c.Conn.Write([]byte{1})
	})
	return c.err
}
func (c *authenticatedConn) Read(p []byte) (int, error) {
	if err := c.authenticate(); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}
func (c *authenticatedConn) Write(p []byte) (int, error) {
	if err := c.authenticate(); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
func (c *authenticatedConn) Close() error { c.release(); return c.Conn.Close() }
func (c *authenticatedConn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline = t
	c.writeDeadline = t
	return c.Conn.SetDeadline(t)
}
func (c *authenticatedConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline = t
	return c.Conn.SetReadDeadline(t)
}
func (c *authenticatedConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.writeDeadline = t
	return c.Conn.SetWriteDeadline(t)
}

// DialTimeout discovers and authenticates the endpoint at path. The timeout
// covers dialing, TLS and client authentication, not subsequent IPC frames.
func DialTimeout(path string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return DialContext(ctx, path)
}

// DialContext never sends a control frame or token before verifying the server
// against the private rendezvous file's certificate. HTTPS_PROXY is deliberately
// irrelevant: control traffic stays on the same machine's loopback interface.
func DialContext(ctx context.Context, path string) (net.Conn, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSocket != 0 {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}
	endpoint, err := readEndpoint(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(endpoint.Certificate)) {
		return nil, errors.New("invalid control endpoint certificate")
	}
	token, err := base64.StdEncoding.DecodeString(endpoint.Token)
	if err != nil || len(token) != 32 {
		return nil, errors.New("invalid control endpoint credential")
	}
	// Bound authentication even for callers with an unbounded context.
	authCtx, cancel := context.WithTimeout(ctx, authenticationTimeout)
	defer cancel()
	dialer := tls.Dialer{Config: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}}
	c, err := dialer.DialContext(authCtx, "tcp4", endpoint.Address)
	if err != nil {
		return nil, fmt.Errorf("control TLS connection: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			c.Close()
		}
	}()
	deadline, _ := authCtx.Deadline()
	_ = c.SetDeadline(deadline)
	stop := context.AfterFunc(authCtx, func() { _ = c.SetDeadline(time.Now()) })
	if _, err = c.Write(token); err == nil {
		var ack [1]byte
		_, err = io.ReadFull(c, ack[:])
		if err == nil && ack[0] != 1 {
			err = errors.New("control authentication failed")
		}
	}
	if !stop() {
		return nil, errors.New("control authentication canceled")
	}
	if err != nil {
		return nil, errors.New("control authentication failed")
	}
	if err = c.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	ok = true
	return c, nil
}

func readEndpoint(path string) (controlEndpoint, error) {
	var endpoint controlEndpoint
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return endpoint, errors.New("cannot read private control endpoint")
	}
	defer f.Close()
	if err = checkPrivateFile(f); err != nil {
		return endpoint, err
	}
	dec := json.NewDecoder(io.LimitReader(f, 16384))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&endpoint); err != nil {
		return endpoint, errors.New("invalid control endpoint")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return endpoint, errors.New("invalid control endpoint trailing data")
	}
	host, port, err := net.SplitHostPort(endpoint.Address)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || host != "127.0.0.1" || portErr != nil || portNumber < 1 || portNumber > 65535 || endpoint.Version != endpointVersion {
		return endpoint, errors.New("invalid loopback control endpoint")
	}
	return endpoint, nil
}

func checkPrivateFile(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || stat.Uid != uint32(os.Geteuid()) || info.Size() > 16384 {
		return errors.New("control file must be a small owner-only regular file owned by this user")
	}
	return nil
}

func newControlCertificate() (tls.Certificate, []byte, error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Entmoot local control"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(10, 0, 0), IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}
