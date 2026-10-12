package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	registryv1 "github.com/onlyboxes/onlyboxes/api/gen/go/registry/v1"
	"github.com/onlyboxes/onlyboxes/worker/worker-kubernetes/internal/config"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type session struct {
	id, name string
	expires  time.Time
	ready    chan struct{}
	initErr  error
	inflight int
	retiring bool
}
type sessionManager struct {
	mu       sync.Mutex
	sessions map[string]*session
	backend  *podBackend
	cfg      config.Config
	orphans  map[string]struct{}
}

func newSessionManager(b *podBackend, c config.Config) *sessionManager {
	return &sessionManager{sessions: map[string]*session{}, orphans: map[string]struct{}{}, backend: b, cfg: c}
}
func sessionName(id string) string {
	hash := sha256.Sum256([]byte(id))
	return "ob-terminal-" + hex.EncodeToString(hash[:])[:48]
}
func (m *sessionManager) ttl(n *int) time.Duration {
	v := m.cfg.TerminalLeaseDefaultSec
	if n != nil {
		v = *n
	}
	if v < m.cfg.TerminalLeaseMinSec {
		v = m.cfg.TerminalLeaseMinSec
	}
	if v > m.cfg.TerminalLeaseMaxSec {
		v = m.cfg.TerminalLeaseMaxSec
	}
	return time.Duration(v) * time.Second
}
func (m *sessionManager) acquire(ctx context.Context, id string, create bool, ttl *int) (*session, bool, error) {
	return m.claim(ctx, id, create, ttl, true)
}

func (m *sessionManager) claim(ctx context.Context, id string, create bool, ttl *int, extend bool) (*session, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if ttl != nil && *ttl <= 0 {
		return nil, false, newTerminalExecError(terminalExecCodeInvalidPayload, "lease_ttl_sec must be positive")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		id = uuid.NewString()
		create = true
	}
	m.mu.Lock()
	s := m.sessions[id]
	created := false
	if s == nil {
		if !create {
			m.mu.Unlock()
			return nil, false, newTerminalExecError(terminalExecCodeSessionNotFound, terminalExecNoSessionMessage)
		}
		if m.cfg.TerminalMaxActiveSessions > 0 && len(m.sessions) >= m.cfg.TerminalMaxActiveSessions {
			m.mu.Unlock()
			return nil, false, newTerminalExecError(terminalExecCodeSessionCapacityExceeded, terminalExecCapacityMessage)
		}
		s = &session{id: id, name: sessionName(id), expires: time.Now().Add(m.ttl(ttl)), ready: make(chan struct{})}
		m.sessions[id] = s
		created = true
	}
	if s.retiring || time.Now().After(s.expires) {
		m.mu.Unlock()
		return nil, false, newTerminalExecError(terminalExecCodeSessionNotFound, terminalExecNoSessionMessage)
	}
	max := m.cfg.TerminalSessionMaxInflight
	if max <= 0 {
		max = defaultTerminalSessionMaxInflight
	}
	if s.inflight >= max {
		m.mu.Unlock()
		return nil, false, newTerminalExecError(terminalExecCodeSessionBusy, terminalExecBusyMessage)
	}
	s.inflight++
	expiry := time.Now().Add(m.ttl(ttl))
	if extend && expiry.After(s.expires) {
		s.expires = expiry
	}
	m.mu.Unlock()
	if created {
		err := m.backend.create(ctx, s.name, m.cfg.TerminalExecImage, m.cfg.TerminalExecMemoryLimit, m.cfg.TerminalExecCPULimit)
		if err == nil {
			err = m.backend.ready(ctx, s.name)
		}
		m.mu.Lock()
		s.initErr = err
		if err != nil {
			s.retiring = true
		}
		close(s.ready)
		m.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		m.release(s)
		return nil, created, ctx.Err()
	case <-s.ready:
	}
	m.mu.Lock()
	err := s.initErr
	retiring := s.retiring
	m.mu.Unlock()
	if err != nil || retiring {
		m.release(s)
		if err == nil {
			err = newTerminalExecError(terminalExecCodeSessionNotFound, terminalExecNoSessionMessage)
		}
		return nil, created, err
	}
	return s, created, nil
}
func (m *sessionManager) release(s *session) { m.mu.Lock(); s.inflight--; m.mu.Unlock() }
func (m *sessionManager) execute(ctx context.Context, r terminalExecRequest) (terminalExecRunResult, error) {
	s, created, err := m.acquire(ctx, r.SessionID, r.CreateIfMissing, r.LeaseTTLSec)
	if err != nil {
		return terminalExecRunResult{}, err
	}
	defer m.release(s)
	out := &boundedBuffer{limit: m.cfg.TerminalOutputLimitBytes}
	stderr := &boundedBuffer{limit: m.cfg.TerminalOutputLimitBytes}
	code, err := m.backend.exec(ctx, s.name, []string{"sh", "-lc", r.Command}, nil, out, stderr)
	if err != nil {
		m.mu.Lock()
		s.retiring = true
		m.mu.Unlock()
		return terminalExecRunResult{}, err
	}
	m.mu.Lock()
	expires := s.expires.UnixMilli()
	m.mu.Unlock()
	return terminalExecRunResult{SessionID: s.id, Created: created, Stdout: out.String(), Stderr: stderr.String(), ExitCode: code, StdoutTruncated: out.truncated, StderrTruncated: stderr.truncated, LeaseExpiresUnixMS: expires}, nil
}
func (m *sessionManager) renew(ctx context.Context, r terminalLeaseRenewPayload) (terminalLeaseRenewResult, error) {
	if err := ctx.Err(); err != nil {
		return terminalLeaseRenewResult{}, err
	}
	if r.LeaseTTLSec <= 0 {
		return terminalLeaseRenewResult{}, newTerminalExecError(terminalExecCodeInvalidPayload, "lease_ttl_sec must be positive")
	}
	r.SessionID = strings.TrimSpace(r.SessionID)
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[r.SessionID]
	if s == nil || s.retiring || !s.expires.After(time.Now()) {
		return terminalLeaseRenewResult{}, newTerminalExecError(terminalExecCodeSessionNotFound, terminalExecNoSessionMessage)
	}
	expiry := time.Now().Add(m.ttl(&r.LeaseTTLSec))
	if expiry.After(s.expires) {
		s.expires = expiry
	}
	return terminalLeaseRenewResult{SessionID: s.id, LeaseExpiresUnixMS: s.expires.UnixMilli()}, nil
}
func (m *sessionManager) count() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int32(len(m.sessions))
}
func (m *sessionManager) janitor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweep(ctx)
		}
	}
}
func (m *sessionManager) sweep(ctx context.Context) {
	m.mu.Lock()
	var doomed []*session
	var orphanNames []string
	for name := range m.orphans {
		orphanNames = append(orphanNames, name)
	}
	for _, s := range m.sessions {
		if s.retiring || !s.expires.After(time.Now()) {
			s.retiring = true
			doomed = append(doomed, s)
		}
	}
	m.mu.Unlock()
	for _, name := range orphanNames {
		m.cleanupPython(name)
	}
	for _, s := range doomed {
		select {
		case <-s.ready:
		default:
			continue
		}
		cleanup, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := m.backend.remove(cleanup, s.name)
		cancel()
		if err == nil {
			m.mu.Lock()
			if m.sessions[s.id] == s {
				delete(m.sessions, s.id)
			}
			m.mu.Unlock()
		}
	}
}
func (m *sessionManager) python(ctx context.Context, code string) (pythonExecRunResult, error) {
	name := "ob-python-" + uuid.NewString()
	defer m.cleanupPython(name)
	err := m.backend.create(ctx, name, m.cfg.PythonExecImage, m.cfg.PythonExecMemoryLimit, m.cfg.PythonExecCPULimit)
	if err != nil {
		return pythonExecRunResult{}, err
	}
	if err = m.backend.ready(ctx, name); err != nil {
		return pythonExecRunResult{}, err
	}
	out := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	status, err := m.backend.exec(ctx, name, []string{"sh", "-c", `cat > /tmp/script.py && uv run /tmp/script.py`}, strings.NewReader(code), out, stderr)
	return pythonExecRunResult{Output: out.String(), Stderr: stderr.String(), ExitCode: status}, err
}

func (m *sessionManager) cleanupPython(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := m.backend.remove(ctx, name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.orphans[name] = struct{}{}
	} else {
		delete(m.orphans, name)
	}
}
func (m *sessionManager) recover(ctx context.Context, candidates []*registryv1.TerminalSessionRecoveryCandidate) ([]*registryv1.TerminalSessionRecoveryResult, error) {
	results := make([]*registryv1.TerminalSessionRecoveryResult, 0, len(candidates))
	expected := map[string]bool{}
	for _, c := range candidates {
		if c == nil {
			continue
		}
		id := strings.TrimSpace(c.GetSessionId())
		name := sessionName(id)
		expires := time.UnixMilli(c.GetLeaseExpiresUnixMs())
		state := registryv1.TerminalSessionRecoveryResult_INVALID
		if id != "" && expires.After(time.Now()) {
			pod, err := m.backend.owned(ctx, name)
			if apierrors.IsNotFound(err) {
				state = registryv1.TerminalSessionRecoveryResult_MISSING
			} else if err != nil {
				// Do not tell Console that a session was lost because its API is temporarily unavailable.
				return nil, err
			} else if pod.DeletionTimestamp == nil && pod.Status.Phase != "Failed" && pod.Status.Phase != "Succeeded" {
				if err = m.backend.ready(ctx, name); err != nil {
					return nil, err
				}
				ready := make(chan struct{})
				close(ready)
				m.mu.Lock()
				old := m.sessions[id]
				if old == nil {
					m.sessions[id] = &session{id: id, name: name, expires: expires, ready: ready}
				} else if old.retiring {
					m.mu.Unlock()
					results = append(results, &registryv1.TerminalSessionRecoveryResult{SessionId: id, Status: state})
					continue
				} else if expires.After(old.expires) {
					old.expires = expires
				}
				m.mu.Unlock()
				expected[name] = true
				state = registryv1.TerminalSessionRecoveryResult_RECOVERED
			}
		}
		results = append(results, &registryv1.TerminalSessionRecoveryResult{SessionId: id, Status: state})
	}
	pods, err := m.backend.client.CoreV1().Pods(m.backend.namespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=onlyboxes-sandbox,onlyboxes.io/worker=" + m.backend.worker})
	if err != nil {
		return nil, err
	}
	for _, p := range pods.Items {
		if !expected[p.Name] {
			if err = m.backend.remove(ctx, p.Name); err != nil {
				return nil, err
			}
		}
	}
	m.mu.Lock()
	for id, s := range m.sessions {
		if !expected[s.name] {
			delete(m.sessions, id)
		}
	}
	m.mu.Unlock()
	return results, nil
}
