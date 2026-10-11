package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/SaridakisStamatisChristos/ChronosQueue/internal/model"
)

const CurrentSchema = 3
const DefaultPayloadLimit = 1 << 20

var (
	ErrNotFound   = errors.New("job not found")
	ErrConflict   = errors.New("state conflict")
	ErrStaleLease = errors.New("stale or invalid lease")
	ErrInvalid    = errors.New("invalid request")
)

type AuditRecord struct {
	AtNS   int64       `json:"at_ns"`
	JobID  string      `json:"job_id"`
	From   model.State `json:"from"`
	To     model.State `json:"to"`
	Reason string      `json:"reason"`
}
type diskState struct {
	SchemaVersion int                   `json:"schema_version"`
	NextSeq       uint64                `json:"next_sequence"`
	Jobs          map[string]*model.Job `json:"jobs"`
	Idempotency   map[string]string     `json:"idempotency"`
	Audit         []AuditRecord         `json:"audit"`
}

type Store struct {
	mu           sync.Mutex
	path         string
	lock         *os.File
	st           diskState
	payloadLimit int
	closed       bool
}

type SubmitRequest struct {
	Queue          string
	ScheduledAt    time.Time
	Payload        json.RawMessage
	MaxAttempts    uint32
	RetryPolicy    model.RetryPolicy
	Priority       int32
	Tags           []string
	IdempotencyKey string
}
type ClaimRequest struct {
	Worker string
	Queues []string
	Lease  time.Duration
}
type Lease struct {
	Job   *model.Job `json:"job"`
	Token string     `json:"lease_token"`
}
type CompleteRequest struct {
	JobID, Worker, Token string
	Generation           uint64
	Result               json.RawMessage
}
type FailRequest struct {
	JobID, Worker, Token string
	Generation           uint64
	Error                string
	Permanent            bool
}
type ListFilter struct {
	Queue                           string
	State                           model.State
	Tags                            []string
	MinPriority                     *int32
	MaxPriority                     *int32
	LeaseOwner                      string
	CreatedAfterNS, CreatedBeforeNS int64
	Limit                           int
	Cursor                          string
}
type Page struct {
	Jobs       []*model.Job `json:"jobs"`
	NextCursor string       `json:"next_cursor,omitempty"`
}
type cursor struct {
	AfterSeq    uint64 `json:"after_seq"`
	SnapshotSeq uint64 `json:"snapshot_seq"`
}

func Open(path string, payloadLimit int) (*Store, error) {
	if payloadLimit <= 0 {
		payloadLimit = DefaultPayloadLimit
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("storage already locked: %w", err)
	}
	s := &Store{path: path, lock: lf, payloadLimit: payloadLimit}
	if err = s.load(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	return s.lock.Close()
}
func initialState() diskState {
	return diskState{SchemaVersion: CurrentSchema, NextSeq: 1, Jobs: map[string]*model.Job{}, Idempotency: map[string]string{}, Audit: []AuditRecord{}}
}
func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.st = initialState()
		return s.persist(s.st)
	}
	if err != nil {
		return err
	}
	var st diskState
	if err = json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("corrupt state: %w", err)
	}
	if st.SchemaVersion < 1 || st.SchemaVersion > CurrentSchema {
		return fmt.Errorf("unsupported schema version %d", st.SchemaVersion)
	}
	if st.Jobs == nil {
		st.Jobs = map[string]*model.Job{}
	}
	if err = migrateState(&st, CurrentSchema); err != nil {
		return err
	}
	if st.Idempotency == nil {
		st.Idempotency = rebuildIdempotency(st.Jobs)
	}
	if st.NextSeq == 0 {
		st.NextSeq = 1
	}
	if err = validateState(st); err != nil {
		return fmt.Errorf("invalid persisted state: %w", err)
	}
	s.st = st
	return s.persist(st)
}
func (s *Store) transact(fn func(*diskState) error) error {
	return s.transactMaybe(func(st *diskState) (bool, error) {
		if err := fn(st); err != nil {
			return false, err
		}
		return true, nil
	})
}
func (s *Store) transactMaybe(fn func(*diskState) (bool, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("store closed")
	}
	n := cloneState(s.st)
	changed, err := fn(&n)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err := validateState(n); err != nil {
		return err
	}
	if err := s.persist(n); err != nil {
		return err
	}
	s.st = n
	return nil
}
func (s *Store) view(fn func(diskState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("store closed")
	}
	return fn(cloneState(s.st))
}
func cloneState(st diskState) diskState {
	n := diskState{SchemaVersion: st.SchemaVersion, NextSeq: st.NextSeq, Jobs: map[string]*model.Job{}, Idempotency: map[string]string{}, Audit: append([]AuditRecord(nil), st.Audit...)}
	for k, v := range st.Jobs {
		n.Jobs[k] = v.Clone()
	}
	for k, v := range st.Idempotency {
		n.Idempotency[k] = v
	}
	return n
}
func (s *Store) persist(st diskState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".chronosqueue-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(tmp)
		}
	}()
	if err = f.Chmod(0o600); err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, s.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return err
	}
	ok = true
	return nil
}
func (s *Store) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("closed")
	}
	dirPath := filepath.Dir(s.path)
	f, err := os.CreateTemp(dirPath, ".ready-*")
	if err != nil {
		return err
	}
	n := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			_ = os.Remove(n)
		}
	}()
	if _, err = f.Write([]byte{1}); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Remove(n); err != nil {
		return err
	}
	d, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return err
	}
	ok = true
	return nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func token() (string, string, error) {
	var b [32]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", "", err
	}
	raw := base64.RawURLEncoding.EncodeToString(b[:])
	h := sha256.Sum256([]byte(raw))
	return raw, hex.EncodeToString(h[:]), nil
}
func tokenHash(raw string) string { h := sha256.Sum256([]byte(raw)); return hex.EncodeToString(h[:]) }
func idemKey(q, k string) string  { return q + "\x00" + k }
func cleanTags(tags []string) ([]string, error) {
	if len(tags) > 64 {
		return nil, fmt.Errorf("%w: too many tags", ErrInvalid)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" || len(t) > 128 {
			return nil, fmt.Errorf("%w: invalid tag", ErrInvalid)
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) Submit(req SubmitRequest, now time.Time) (*model.Job, bool, error) {
	if strings.TrimSpace(req.Queue) == "" || len(req.Queue) > 128 {
		return nil, false, fmt.Errorf("%w: invalid queue", ErrInvalid)
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		return nil, false, fmt.Errorf("%w: payload must be valid JSON", ErrInvalid)
	}
	if len(req.Payload) > s.payloadLimit {
		return nil, false, fmt.Errorf("%w: payload too large", ErrInvalid)
	}
	if req.MaxAttempts == 0 || req.MaxAttempts > 1000 {
		return nil, false, fmt.Errorf("%w: max attempts out of range", ErrInvalid)
	}
	if req.Priority < -1000000 || req.Priority > 1000000 {
		return nil, false, fmt.Errorf("%w: priority out of range", ErrInvalid)
	}
	if err := req.RetryPolicy.Validate(); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	tags, err := cleanTags(req.Tags)
	if err != nil {
		return nil, false, err
	}
	if req.ScheduledAt.IsZero() {
		req.ScheduledAt = now
	}
	req.ScheduledAt = req.ScheduledAt.UTC()
	now = now.UTC()
	var out *model.Job
	duplicate := false
	err = s.transact(func(st *diskState) error {
		if req.IdempotencyKey != "" {
			if len(req.IdempotencyKey) > 256 {
				return fmt.Errorf("%w: idempotency key too long", ErrInvalid)
			}
			if id, ok := st.Idempotency[idemKey(req.Queue, req.IdempotencyKey)]; ok {
				out = st.Jobs[id].Clone()
				duplicate = true
				return nil
			}
		}
		id, e := newID()
		if e != nil {
			return e
		}
		seq := st.NextSeq
		st.NextSeq++
		state := model.Scheduled
		if !req.ScheduledAt.After(now) {
			state = model.Ready
		}
		j := &model.Job{ID: id, Sequence: seq, Queue: req.Queue, CreatedAtNS: now.UnixNano(), ScheduledAtNS: req.ScheduledAt.UnixNano(), Payload: append(json.RawMessage(nil), req.Payload...), State: state, MaxAttempts: req.MaxAttempts, RetryPolicy: req.RetryPolicy, Priority: req.Priority, Tags: tags, IdempotencyKey: req.IdempotencyKey}
		st.Jobs[id] = j
		if req.IdempotencyKey != "" {
			st.Idempotency[idemKey(req.Queue, req.IdempotencyKey)] = id
		}
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: id, To: state, Reason: "submit"})
		out = j.Clone()
		return nil
	})
	return out, duplicate, err
}

func normalize(st *diskState, now time.Time) (expired int, retried int, dead int, changed bool) {
	ns := now.UTC().UnixNano()
	for _, j := range st.Jobs {
		if j.State == model.Scheduled && j.ScheduledAtNS <= ns {
			changed = true
			from := j.State
			j.State = model.Ready
			st.Audit = append(st.Audit, AuditRecord{AtNS: ns, JobID: j.ID, From: from, To: j.State, Reason: "schedule_due"})
		}
		if j.State == model.Leased && j.LeaseExpirationNS <= ns {
			changed = true
			expired++
			from := j.State
			expiredAtNS := j.LeaseExpirationNS
			j.LeaseOwner = ""
			j.LeaseExpirationNS = 0
			j.LeaseTokenHash = ""
			j.Failure = "lease expired"
			if j.AttemptCount >= j.MaxAttempts {
				j.State = model.DeadLettered
				j.TerminalAtNS = ns
				dead++
				st.Audit = append(st.Audit, AuditRecord{AtNS: ns, JobID: j.ID, From: from, To: j.State, Reason: "lease_expired_max_attempts"})
			} else {
				j.State = model.Scheduled
				j.ScheduledAtNS = time.Unix(0, expiredAtNS).UTC().Add(j.RetryPolicy.Delay(j.AttemptCount)).UnixNano()
				if j.ScheduledAtNS <= ns {
					j.State = model.Ready
				}
				retried++
				st.Audit = append(st.Audit, AuditRecord{AtNS: ns, JobID: j.ID, From: from, To: j.State, Reason: "lease_expired_retry"})
			}
		}
	}
	return
}
func (s *Store) Reap(now time.Time) (int, int, int, error) {
	var e, r, d int
	err := s.transactMaybe(func(st *diskState) (bool, error) {
		var changed bool
		e, r, d, changed = normalize(st, now)
		return changed, nil
	})
	return e, r, d, err
}

func (s *Store) Claim(req ClaimRequest, now time.Time) (*Lease, int, int, int, error) {
	if strings.TrimSpace(req.Worker) == "" || len(req.Worker) > 128 {
		return nil, 0, 0, 0, fmt.Errorf("%w: invalid worker", ErrInvalid)
	}
	if req.Lease < time.Millisecond || req.Lease > 24*time.Hour {
		return nil, 0, 0, 0, fmt.Errorf("%w: lease duration out of range", ErrInvalid)
	}
	qset := map[string]bool{}
	for _, q := range req.Queues {
		if strings.TrimSpace(q) == "" {
			return nil, 0, 0, 0, fmt.Errorf("%w: invalid queue", ErrInvalid)
		}
		qset[q] = true
	}
	var lease *Lease
	var ex, re, dl int
	err := s.transactMaybe(func(st *diskState) (bool, error) {
		var normalized bool
		ex, re, dl, normalized = normalize(st, now)
		var cand []*model.Job
		for _, j := range st.Jobs {
			if j.State == model.Ready && j.ScheduledAtNS <= now.UnixNano() && (len(qset) == 0 || qset[j.Queue]) {
				cand = append(cand, j)
			}
		}
		sort.Slice(cand, func(i, j int) bool {
			a, b := cand[i], cand[j]
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			if a.ScheduledAtNS != b.ScheduledAtNS {
				return a.ScheduledAtNS < b.ScheduledAtNS
			}
			if a.Sequence != b.Sequence {
				return a.Sequence < b.Sequence
			}
			return a.ID < b.ID
		})
		if len(cand) == 0 {
			return normalized, nil
		}
		j := cand[0]
		raw, h, e := token()
		if e != nil {
			return false, e
		}
		from := j.State
		j.State = model.Leased
		j.AttemptCount++
		j.LeaseOwner = req.Worker
		j.LeaseExpirationNS = now.Add(req.Lease).UnixNano()
		j.LeaseGeneration++
		j.LeaseTokenHash = h
		j.Failure = ""
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "claim"})
		lease = &Lease{Job: j.Clone(), Token: raw}
		return true, nil
	})
	return lease, ex, re, dl, err
}

func (s *Store) Heartbeat(jobID, worker, raw string, generation uint64, extend time.Duration, now time.Time) (*model.Job, error) {
	if extend < time.Millisecond || extend > 24*time.Hour {
		return nil, fmt.Errorf("%w: heartbeat extension out of range", ErrInvalid)
	}
	var out *model.Job
	err := s.transact(func(st *diskState) error {
		normalize(st, now)
		j := st.Jobs[jobID]
		if j == nil {
			return ErrNotFound
		}
		if j.State != model.Leased || j.LeaseOwner != worker || j.LeaseGeneration != generation || j.LeaseTokenHash != tokenHash(raw) || j.LeaseExpirationNS <= now.UnixNano() {
			return ErrStaleLease
		}
		j.LeaseExpirationNS = now.Add(extend).UnixNano()
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: j.State, To: j.State, Reason: "heartbeat"})
		out = j.Clone()
		return nil
	})
	return out, err
}

func duplicateResolution(j *model.Job, g uint64, raw, op string) bool {
	return j.LastResolution != nil && j.LastResolution.Generation == g && j.LastResolution.TokenHash == tokenHash(raw) && j.LastResolution.Operation == op
}
func validateLease(j *model.Job, g uint64, worker, raw string, now time.Time) error {
	if j.State != model.Leased || j.LeaseGeneration != g || j.LeaseOwner != worker || j.LeaseTokenHash != tokenHash(raw) || j.LeaseExpirationNS <= now.UnixNano() {
		return ErrStaleLease
	}
	return nil
}

func (s *Store) Complete(req CompleteRequest, now time.Time) (*model.Job, bool, error) {
	var out *model.Job
	idem := false
	err := s.transact(func(st *diskState) error {
		j := st.Jobs[req.JobID]
		if j == nil {
			return ErrNotFound
		}
		if duplicateResolution(j, req.Generation, req.Token, "complete") {
			out = j.Clone()
			idem = true
			return nil
		}
		normalize(st, now)
		j = st.Jobs[req.JobID]
		if err := validateLease(j, req.Generation, req.Worker, req.Token, now); err != nil {
			return err
		}
		if len(req.Result) > 0 && !json.Valid(req.Result) {
			return fmt.Errorf("%w: invalid result JSON", ErrInvalid)
		}
		from := j.State
		h := j.LeaseTokenHash
		j.State = model.Succeeded
		j.Result = append(json.RawMessage(nil), req.Result...)
		j.Failure = ""
		j.TerminalAtNS = now.UnixNano()
		j.LeaseOwner = ""
		j.LeaseExpirationNS = 0
		j.LeaseTokenHash = ""
		j.LastResolution = &model.Resolution{Generation: req.Generation, TokenHash: h, Operation: "complete", StateAfter: j.State, AtNS: now.UnixNano()}
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "complete"})
		out = j.Clone()
		return nil
	})
	return out, idem, err
}

func (s *Store) Fail(req FailRequest, now time.Time) (*model.Job, bool, bool, error) {
	var out *model.Job
	var idem, retry bool
	err := s.transact(func(st *diskState) error {
		j := st.Jobs[req.JobID]
		if j == nil {
			return ErrNotFound
		}
		if duplicateResolution(j, req.Generation, req.Token, "fail") {
			out = j.Clone()
			idem = true
			return nil
		}
		normalize(st, now)
		j = st.Jobs[req.JobID]
		if err := validateLease(j, req.Generation, req.Worker, req.Token, now); err != nil {
			return err
		}
		from := j.State
		h := j.LeaseTokenHash
		j.LeaseOwner = ""
		j.LeaseExpirationNS = 0
		j.LeaseTokenHash = ""
		j.Failure = req.Error
		j.LastResolution = &model.Resolution{Generation: req.Generation, TokenHash: h, Operation: "fail", AtNS: now.UnixNano()}
		if req.Permanent && j.RetryPolicy.AllowPermanentFailure {
			j.State = model.Failed
			j.TerminalAtNS = now.UnixNano()
			j.LastResolution.StateAfter = j.State
		} else if j.AttemptCount >= j.MaxAttempts {
			j.State = model.DeadLettered
			j.TerminalAtNS = now.UnixNano()
			j.LastResolution.StateAfter = j.State
		} else {
			j.State = model.Scheduled
			j.ScheduledAtNS = now.Add(j.RetryPolicy.Delay(j.AttemptCount)).UnixNano()
			if j.ScheduledAtNS <= now.UnixNano() {
				j.State = model.Ready
			}
			retry = true
			j.LastResolution.StateAfter = j.State
		}
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "fail"})
		out = j.Clone()
		return nil
	})
	return out, idem, retry, err
}

func (s *Store) Cancel(id string, now time.Time) (*model.Job, error) {
	var out *model.Job
	err := s.transact(func(st *diskState) error {
		normalize(st, now)
		j := st.Jobs[id]
		if j == nil {
			return ErrNotFound
		}
		if j.State == model.Cancelled {
			out = j.Clone()
			return nil
		}
		if j.State == model.Succeeded || j.State == model.DeadLettered || j.State == model.Failed {
			return ErrConflict
		}
		from := j.State
		j.State = model.Cancelled
		j.TerminalAtNS = now.UnixNano()
		j.LeaseOwner = ""
		j.LeaseExpirationNS = 0
		j.LeaseTokenHash = ""
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "cancel"})
		out = j.Clone()
		return nil
	})
	return out, err
}
func (s *Store) Requeue(id string, now time.Time) (*model.Job, error) {
	var out *model.Job
	err := s.transact(func(st *diskState) error {
		normalize(st, now)
		j := st.Jobs[id]
		if j == nil {
			return ErrNotFound
		}
		if j.State != model.DeadLettered && j.State != model.Failed && j.State != model.Cancelled {
			return ErrConflict
		}
		from := j.State
		j.State = model.Ready
		j.AttemptCount = 0
		j.ScheduledAtNS = now.UnixNano()
		j.TerminalAtNS = 0
		j.Failure = ""
		j.Result = nil
		j.LeaseOwner = ""
		j.LeaseExpirationNS = 0
		j.LeaseTokenHash = ""
		j.LastResolution = nil
		st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "explicit_requeue"})
		out = j.Clone()
		return nil
	})
	return out, err
}
func (s *Store) RequeueMany(ids []string, now time.Time) ([]*model.Job, error) {
	if len(ids) == 0 || len(ids) > 100 {
		return nil, fmt.Errorf("%w: requeue selection must contain 1..100 job IDs", ErrInvalid)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("%w: duplicate or empty job ID", ErrInvalid)
		}
		seen[id] = true
	}
	var out []*model.Job
	err := s.transact(func(st *diskState) error {
		normalize(st, now)
		for _, id := range ids {
			j := st.Jobs[id]
			if j == nil {
				return ErrNotFound
			}
			if j.State != model.DeadLettered && j.State != model.Failed && j.State != model.Cancelled {
				return ErrConflict
			}
		}
		for _, id := range ids {
			j := st.Jobs[id]
			from := j.State
			j.State = model.Ready
			j.AttemptCount = 0
			j.ScheduledAtNS = now.UnixNano()
			j.TerminalAtNS = 0
			j.Failure = ""
			j.Result = nil
			j.LeaseOwner = ""
			j.LeaseExpirationNS = 0
			j.LeaseTokenHash = ""
			j.LastResolution = nil
			st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: j.ID, From: from, To: j.State, Reason: "explicit_batch_requeue"})
			out = append(out, j.Clone())
		}
		return nil
	})
	return out, err
}

func (s *Store) Get(id string, now time.Time) (*model.Job, error) {
	var out *model.Job
	if _, _, _, err := s.Reap(now); err != nil {
		return nil, err
	}
	err := s.view(func(st diskState) error {
		j := st.Jobs[id]
		if j == nil {
			return ErrNotFound
		}
		out = j.Clone()
		return nil
	})
	return out, err
}

func match(j *model.Job, f ListFilter) bool {
	if f.Queue != "" && j.Queue != f.Queue {
		return false
	}
	if f.State != "" && j.State != f.State {
		return false
	}
	if f.LeaseOwner != "" && j.LeaseOwner != f.LeaseOwner {
		return false
	}
	if f.MinPriority != nil && j.Priority < *f.MinPriority {
		return false
	}
	if f.MaxPriority != nil && j.Priority > *f.MaxPriority {
		return false
	}
	if f.CreatedAfterNS != 0 && j.CreatedAtNS <= f.CreatedAfterNS {
		return false
	}
	if f.CreatedBeforeNS != 0 && j.CreatedAtNS >= f.CreatedBeforeNS {
		return false
	}
	if len(f.Tags) > 0 {
		have := map[string]bool{}
		for _, t := range j.Tags {
			have[t] = true
		}
		for _, t := range f.Tags {
			if !have[t] {
				return false
			}
		}
	}
	return true
}
func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeCursor(v string) (cursor, error) {
	if v == "" {
		return cursor{}, nil
	}
	b, e := base64.RawURLEncoding.DecodeString(v)
	if e != nil {
		return cursor{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	var c cursor
	if e = json.Unmarshal(b, &c); e != nil || c.SnapshotSeq == 0 || c.AfterSeq > c.SnapshotSeq {
		return cursor{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
	}
	return c, nil
}
func (s *Store) List(f ListFilter, now time.Time) (Page, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 500 {
		return Page{}, fmt.Errorf("%w: limit too large", ErrInvalid)
	}
	c, err := decodeCursor(f.Cursor)
	if err != nil {
		return Page{}, err
	}
	if _, _, _, err = s.Reap(now); err != nil {
		return Page{}, err
	}
	var p Page
	err = s.view(func(st diskState) error {
		snap := c.SnapshotSeq
		maxSnap := st.NextSeq - 1
		if snap == 0 {
			snap = maxSnap
		} else if snap > maxSnap {
			return fmt.Errorf("%w: cursor snapshot is from the future", ErrInvalid)
		}
		var rows []*model.Job
		for _, j := range st.Jobs {
			if j.Sequence > c.AfterSeq && j.Sequence <= snap && match(j, f) {
				rows = append(rows, j.Clone())
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
		if len(rows) > f.Limit {
			p.Jobs = rows[:f.Limit]
			p.NextCursor = encodeCursor(cursor{AfterSeq: p.Jobs[len(p.Jobs)-1].Sequence, SnapshotSeq: snap})
		} else {
			p.Jobs = rows
		}
		return nil
	})
	return p, err
}

func (s *Store) Purge(before time.Time, limit int, now time.Time) (int, error) {
	if limit <= 0 || limit > 10000 {
		return 0, fmt.Errorf("%w: purge limit out of range", ErrInvalid)
	}
	n := 0
	err := s.transact(func(st *diskState) error {
		normalize(st, now)
		ids := []string{}
		for id, j := range st.Jobs {
			if j.State.Terminal() && j.TerminalAtNS > 0 && j.TerminalAtNS < before.UnixNano() {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		if len(ids) > limit {
			ids = ids[:limit]
		}
		for _, id := range ids {
			j := st.Jobs[id]
			delete(st.Jobs, id)
			if j.IdempotencyKey != "" {
				delete(st.Idempotency, idemKey(j.Queue, j.IdempotencyKey))
			}
			st.Audit = append(st.Audit, AuditRecord{AtNS: now.UnixNano(), JobID: id, From: j.State, Reason: "purge"})
			n++
		}
		return nil
	})
	return n, err
}
func (s *Store) QueueCounts(now time.Time) (map[string]map[model.State]uint64, error) {
	if _, _, _, err := s.Reap(now); err != nil {
		return nil, err
	}
	out := map[string]map[model.State]uint64{}
	err := s.view(func(st diskState) error {
		for _, j := range st.Jobs {
			m := out[j.Queue]
			if m == nil {
				m = map[model.State]uint64{}
				out[j.Queue] = m
			}
			m[j.State]++
		}
		return nil
	})
	return out, err
}
func (s *Store) ActiveLeases(now time.Time) ([]*model.Job, error) {
	if _, _, _, err := s.Reap(now); err != nil {
		return nil, err
	}
	var out []*model.Job
	err := s.view(func(st diskState) error {
		for _, j := range st.Jobs {
			if j.State == model.Leased {
				out = append(out, j.Clone())
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].LeaseExpirationNS < out[j].LeaseExpirationNS })
		return nil
	})
	return out, err
}
func (s *Store) Audit() ([]AuditRecord, error) {
	var out []AuditRecord
	err := s.view(func(st diskState) error { out = append([]AuditRecord(nil), st.Audit...); return nil })
	return out, err
}
func validateState(st diskState) error {
	if st.SchemaVersion < 1 || st.SchemaVersion > CurrentSchema {
		return errors.New("unsupported schema")
	}
	if st.NextSeq == 0 {
		return errors.New("next sequence is zero")
	}
	seenSeq := map[uint64]bool{}
	var maxSeq uint64
	for id, j := range st.Jobs {
		if id == "" || j == nil || j.ID != id {
			return errors.New("job id mismatch")
		}
		if strings.TrimSpace(j.Queue) == "" || len(j.Queue) > 128 {
			return errors.New("invalid persisted queue")
		}
		if !json.Valid(j.Payload) {
			return errors.New("invalid persisted payload")
		}
		if j.MaxAttempts == 0 || j.MaxAttempts > 1000 || j.AttemptCount > j.MaxAttempts {
			return errors.New("invalid persisted attempt accounting")
		}
		if err := j.RetryPolicy.Validate(); err != nil {
			return fmt.Errorf("invalid persisted retry policy: %w", err)
		}
		if j.Priority < -1000000 || j.Priority > 1000000 {
			return errors.New("invalid persisted priority")
		}
		switch j.State {
		case model.Scheduled, model.Ready, model.Leased, model.Succeeded, model.Failed, model.DeadLettered, model.Cancelled:
		default:
			return errors.New("unknown persisted job state")
		}
		if j.Sequence == 0 || seenSeq[j.Sequence] {
			return errors.New("duplicate or zero sequence")
		}
		seenSeq[j.Sequence] = true
		if j.Sequence > maxSeq {
			maxSeq = j.Sequence
		}
		if j.State == model.Leased {
			if j.LeaseOwner == "" || j.LeaseExpirationNS == 0 || len(j.LeaseTokenHash) != 64 {
				return errors.New("leased job missing lease metadata")
			}
		} else if j.LeaseOwner != "" || j.LeaseExpirationNS != 0 || j.LeaseTokenHash != "" {
			return errors.New("non-leased job retains lease metadata")
		}
		if j.State.Terminal() && j.TerminalAtNS == 0 {
			return errors.New("terminal job missing terminal timestamp")
		}
		if !j.State.Terminal() && j.TerminalAtNS != 0 {
			return errors.New("non-terminal job has terminal timestamp")
		}
	}
	if maxSeq >= st.NextSeq {
		return errors.New("next sequence does not exceed existing rows")
	}
	if st.Idempotency != nil {
		for k, id := range st.Idempotency {
			j := st.Jobs[id]
			if j == nil || j.IdempotencyKey == "" || idemKey(j.Queue, j.IdempotencyKey) != k {
				return errors.New("invalid idempotency index")
			}
		}
		for id, j := range st.Jobs {
			if j.IdempotencyKey != "" {
				if st.Idempotency[idemKey(j.Queue, j.IdempotencyKey)] != id {
					return errors.New("missing idempotency index")
				}
			}
		}
	}
	return nil
}

func rebuildIdempotency(jobs map[string]*model.Job) map[string]string {
	m := map[string]string{}
	for id, j := range jobs {
		if j.IdempotencyKey != "" {
			m[idemKey(j.Queue, j.IdempotencyKey)] = id
		}
	}
	return m
}
