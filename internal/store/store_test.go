package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SaridakisStamatisChristos/ChronosQueue/internal/model"
)

func openTest(t *testing.T) (*Store, string, time.Time) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.json")
	s, e := Open(p, DefaultPayloadLimit)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, p, time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
}
func submitTest(t *testing.T, s *Store, now time.Time, when time.Time, priority int32, key string, max uint32) *model.Job {
	t.Helper()
	if max == 0 {
		max = 3
	}
	j, _, e := s.Submit(SubmitRequest{Queue: "q", ScheduledAt: when, Payload: json.RawMessage(`{"x":1}`), MaxAttempts: max, RetryPolicy: model.RetryPolicy{Kind: model.RetryFixed, BaseDelayNS: int64(time.Second)}, Priority: priority, IdempotencyKey: key}, now)
	if e != nil {
		t.Fatal(e)
	}
	return j
}

func TestDurableAcceptanceSurvivesRestart(t *testing.T) {
	s, p, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s2, e := Open(p, DefaultPayloadLimit)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	got, e := s2.Get(j.ID, now)
	if e != nil || got.ID != j.ID {
		t.Fatalf("restart lost job: %#v %v", got, e)
	}
}

func TestTwentyWorkersRaceForOneJob(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	leases := make(chan *Lease, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			l, _, _, _, e := s.Claim(ClaimRequest{Worker: string(rune('a' + i)), Queues: []string{"q"}, Lease: time.Minute}, now)
			if e != nil {
				errs <- e
				return
			}
			if l != nil {
				leases <- l
			}
		}(i)
	}
	wg.Wait()
	close(leases)
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	var got []*Lease
	for l := range leases {
		got = append(got, l)
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly one lease, got %d", len(got))
	}
	if got[0].Job.ID != j.ID {
		t.Fatalf("wrong job leased")
	}
}

func TestManyWorkersNoOverlappingLeases(t *testing.T) {
	s, _, now := openTest(t)
	for i := 0; i < 80; i++ {
		submitTest(t, s, now, now, 0, "", 3)
	}
	const workers = 24
	var wg sync.WaitGroup
	ids := make(chan string, 80)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				l, _, _, _, e := s.Claim(ClaimRequest{Worker: "w" + string(rune('A'+i)), Queues: []string{"q"}, Lease: time.Hour}, now)
				if e != nil {
					t.Error(e)
					return
				}
				if l == nil {
					return
				}
				ids <- l.Job.ID
			}
		}(i)
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate active lease for %s", id)
		}
		seen[id] = true
	}
	if len(seen) != 80 {
		t.Fatalf("leased %d/80", len(seen))
	}
}

func TestScheduleBoundaryNoPrematureExecution(t *testing.T) {
	s, _, now := openTest(t)
	due := now.Add(10 * time.Second)
	submitTest(t, s, now, due, 0, "", 3)
	l, _, _, _, e := s.Claim(ClaimRequest{Worker: "w", Queues: []string{"q"}, Lease: time.Minute}, due.Add(-time.Nanosecond))
	if e != nil {
		t.Fatal(e)
	}
	if l != nil {
		t.Fatal("job became claimable early")
	}
	l, _, _, _, e = s.Claim(ClaimRequest{Worker: "w", Queues: []string{"q"}, Lease: time.Minute}, due)
	if e != nil || l == nil {
		t.Fatalf("job not claimable at exact due time: %v", e)
	}
}

func TestExpiryReassignmentRejectsStaleCompletion(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	a, _, _, _, e := s.Claim(ClaimRequest{Worker: "a", Queues: []string{"q"}, Lease: time.Second}, now)
	if e != nil {
		t.Fatal(e)
	}
	later := now.Add(2 * time.Second)
	// Lease expiry consumes attempt 1 and applies the configured 1s retry delay.
	later = later.Add(time.Second)
	b, _, _, _, e := s.Claim(ClaimRequest{Worker: "b", Queues: []string{"q"}, Lease: time.Second}, later)
	if e != nil {
		t.Fatal(e)
	}
	if b == nil || b.Job.ID != j.ID || b.Job.AttemptCount != 2 {
		t.Fatalf("bad reassignment %#v", b)
	}
	_, _, e = s.Complete(CompleteRequest{JobID: j.ID, Worker: "a", Token: a.Token, Generation: a.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, later)
	if !errors.Is(e, ErrStaleLease) {
		t.Fatalf("stale completion accepted: %v", e)
	}
	ok, idem, e := s.Complete(CompleteRequest{JobID: j.ID, Worker: "b", Token: b.Token, Generation: b.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, later)
	if e != nil || idem || ok.State != model.Succeeded {
		t.Fatalf("current completion failed: %#v %v", ok, e)
	}
}

func TestCompletionIsIdempotent(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	r := CompleteRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Result: json.RawMessage(`{"ok":true}`)}
	a, idem, e := s.Complete(r, now)
	if e != nil || idem {
		t.Fatalf("first completion: %v idem=%v", e, idem)
	}
	b, idem, e := s.Complete(r, now)
	if e != nil || !idem || a.TerminalAtNS != b.TerminalAtNS {
		t.Fatalf("replay not idempotent: %v idem=%v", e, idem)
	}
}

func TestHeartbeatGenerationSafety(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	a, _, _, _, _ := s.Claim(ClaimRequest{Worker: "a", Lease: time.Second}, now)
	later := now.Add(2 * time.Second)
	later = later.Add(time.Second)
	b, _, _, _, _ := s.Claim(ClaimRequest{Worker: "b", Lease: time.Second}, later)
	if _, e := s.Heartbeat(j.ID, "a", a.Token, a.Job.LeaseGeneration, time.Minute, later); !errors.Is(e, ErrStaleLease) {
		t.Fatalf("stale heartbeat accepted: %v", e)
	}
	if _, e := s.Heartbeat(j.ID, "b", b.Token, b.Job.LeaseGeneration, time.Minute, later); e != nil {
		t.Fatalf("current heartbeat failed: %v", e)
	}
}

func TestRetryMaxAndDeadLetter(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 2)
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	x, _, retry, e := s.Fail(FailRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Error: "boom"}, now)
	if e != nil || !retry || x.State != model.Scheduled {
		t.Fatalf("first failure wrong %#v %v", x, e)
	}
	t2 := now.Add(time.Second)
	l, _, _, _, _ = s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, t2)
	x, _, retry, e = s.Fail(FailRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Error: "boom2"}, t2)
	if e != nil || retry || x.State != model.DeadLettered || x.AttemptCount != 2 {
		t.Fatalf("max attempts wrong %#v %v", x, e)
	}
	if l2, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w2", Lease: time.Minute}, t2.Add(time.Hour)); l2 != nil {
		t.Fatal("dead-lettered job resurrected")
	}
}

func TestCancellationVsCompletionSerialized(t *testing.T) {
	for i := 0; i < 25; i++ {
		s, _, now := openTest(t)
		j := submitTest(t, s, now, now, 0, "", 3)
		l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var ce, xe error
		go func() { defer wg.Done(); <-start; _, ce = s.Cancel(j.ID, now) }()
		go func() {
			defer wg.Done()
			<-start
			_, _, xe = s.Complete(CompleteRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, now)
		}()
		close(start)
		wg.Wait()
		got, e := s.Get(j.ID, now)
		if e != nil {
			t.Fatal(e)
		}
		if got.State != model.Cancelled && got.State != model.Succeeded {
			t.Fatalf("invalid terminal state %s", got.State)
		}
		if got.State == model.Cancelled && xe == nil {
			t.Fatal("completion succeeded after cancellation")
		}
		if got.State == model.Succeeded && ce == nil {
			t.Fatal("cancellation succeeded after completion")
		}
		s.Close()
	}
}

func TestConcurrentIdempotentSubmission(t *testing.T) {
	s, _, now := openTest(t)
	const n = 32
	var wg sync.WaitGroup
	ids := make(chan string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			j, _, e := s.Submit(SubmitRequest{Queue: "q", ScheduledAt: now, Payload: json.RawMessage(`1`), MaxAttempts: 1, RetryPolicy: model.RetryPolicy{Kind: model.RetryFixed}, IdempotencyKey: "same"}, now)
			if e != nil {
				t.Error(e)
				return
			}
			ids <- j.ID
		}()
	}
	wg.Wait()
	close(ids)
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("idempotency produced multiple jobs")
		}
	}
	p, e := s.List(ListFilter{Limit: 100}, now)
	if e != nil || len(p.Jobs) != 1 {
		t.Fatalf("expected one persisted job, got %d %v", len(p.Jobs), e)
	}
}

func TestClaimOrdering(t *testing.T) {
	s, _, now := openTest(t)
	a := submitTest(t, s, now, now, 5, "", 3)
	b := submitTest(t, s, now, now.Add(time.Second), 10, "", 3)
	c := submitTest(t, s, now, now, 10, "", 3)
	_ = a
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now.Add(time.Second))
	if l.Job.ID != c.ID {
		t.Fatalf("expected highest priority earliest scheduled %s got %s (other %s)", c.ID, l.Job.ID, b.ID)
	}
}

func TestKeysetPaginationSnapshotExcludesConcurrentInserts(t *testing.T) {
	s, _, now := openTest(t)
	for i := 0; i < 20; i++ {
		submitTest(t, s, now, now, 0, "", 3)
	}
	p, e := s.List(ListFilter{Limit: 5}, now)
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for _, j := range p.Jobs {
		seen[j.ID] = true
	}
	for i := 0; i < 20; i++ {
		submitTest(t, s, now, now, 0, "", 3)
	}
	cur := p.NextCursor
	for cur != "" {
		p, e = s.List(ListFilter{Limit: 5, Cursor: cur}, now)
		if e != nil {
			t.Fatal(e)
		}
		for _, j := range p.Jobs {
			if seen[j.ID] {
				t.Fatalf("duplicate page item %s", j.ID)
			}
			seen[j.ID] = true
		}
		cur = p.NextCursor
	}
	if len(seen) != 20 {
		t.Fatalf("snapshot pagination returned %d initial jobs, want 20", len(seen))
	}
}

func TestMalformedCursorRejected(t *testing.T) {
	s, _, now := openTest(t)
	_, e := s.List(ListFilter{Cursor: "%%not-base64%%"}, now)
	if !errors.Is(e, ErrInvalid) {
		t.Fatalf("malformed cursor not rejected: %v", e)
	}
}
func TestSecondProcessStoreLockRejected(t *testing.T) {
	s, p, _ := openTest(t)
	_, e := Open(p, DefaultPayloadLimit)
	if e == nil {
		t.Fatal("second writer lock accepted")
	}
	_ = s
}
func TestCorruptStateFailsClosed(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	if e := os.WriteFile(p, []byte(`{"schema_version":3,"jobs":{"x":{"id":"x","sequence":1,"state":"leased"}}}`), 0o600); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(p, DefaultPayloadLimit); e == nil {
		t.Fatal("corrupt leased state opened")
	}
}

func TestMigrationRoundTripAndLegacyRows(t *testing.T) {
	s, p, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "legacy-key", 3)
	s.Close()
	for _, v := range []int{2, 1, 3} {
		if e := Migrate(p, v); e != nil {
			t.Fatalf("migrate to %d: %v", v, e)
		}
		got, e := ReadSchemaVersion(p)
		if e != nil || got != v {
			t.Fatalf("schema got %d want %d err=%v", got, v, e)
		}
	}
	s2, e := Open(p, DefaultPayloadLimit)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	got, dup, e := s2.Submit(SubmitRequest{Queue: "q", ScheduledAt: now, Payload: json.RawMessage(`2`), MaxAttempts: 3, RetryPolicy: model.RetryPolicy{Kind: model.RetryFixed}, IdempotencyKey: "legacy-key"}, now)
	if e != nil || !dup || got.ID != j.ID {
		t.Fatalf("legacy idempotency reconstruction failed %#v dup=%v err=%v", got, dup, e)
	}
}

func TestRetryStormSameFailureIsIdempotent(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	l, _, _, _, err := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	if err != nil {
		t.Fatal(err)
	}
	const n = 24
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _, _, e := s.Fail(FailRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Error: "retry storm"}, now)
			if e != nil {
				errs <- e
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	got, err := s.Get(j.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.AttemptCount != 1 || got.State != model.Scheduled {
		t.Fatalf("storm double-applied failure: %#v", got)
	}
}

func TestCancellationRacesClaim(t *testing.T) {
	for i := 0; i < 25; i++ {
		s, _, now := openTest(t)
		j := submitTest(t, s, now, now, 0, "", 3)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var lease *Lease
		var claimErr, cancelErr error
		go func() {
			defer wg.Done()
			<-start
			lease, _, _, _, claimErr = s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
		}()
		go func() { defer wg.Done(); <-start; _, cancelErr = s.Cancel(j.ID, now) }()
		close(start)
		wg.Wait()
		if claimErr != nil || cancelErr != nil {
			t.Fatalf("race errors claim=%v cancel=%v", claimErr, cancelErr)
		}
		got, err := s.Get(j.ID, now)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != model.Cancelled {
			t.Fatalf("cancellation did not win final state: %s", got.State)
		}
		if lease != nil {
			_, _, e := s.Complete(CompleteRequest{JobID: j.ID, Worker: "w", Token: lease.Token, Generation: lease.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, now)
			if !errors.Is(e, ErrStaleLease) {
				t.Fatalf("cancelled lease completion accepted: %v", e)
			}
		}
		s.Close()
	}
}

func TestPersistenceFailureDoesNotMutateAuthoritativeMemory(t *testing.T) {
	s, _, now := openTest(t)
	original := s.path
	s.path = filepath.Join(t.TempDir(), "missing", "state.json")
	_, _, err := s.Submit(SubmitRequest{Queue: "q", ScheduledAt: now, Payload: json.RawMessage(`1`), MaxAttempts: 1, RetryPolicy: model.RetryPolicy{Kind: model.RetryFixed}}, now)
	if err == nil {
		t.Fatal("submission unexpectedly acknowledged with unavailable storage")
	}
	s.path = original
	p, err := s.List(ListFilter{Limit: 10}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Jobs) != 0 {
		t.Fatalf("failed persistence mutated in-memory state: %d jobs", len(p.Jobs))
	}
}

func TestCorruptUnknownStateFailsClosed(t *testing.T) {
	s, p, now := openTest(t)
	_ = submitTest(t, s, now, now, 0, "", 3)
	s.Close()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), `"state":"ready"`, `"state":"mystery"`, 1))
	if err = os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(p, DefaultPayloadLimit); err == nil {
		t.Fatal("unknown persisted state was accepted")
	}
}

func TestCorruptFutureReadyJobStillCannotExecuteEarly(t *testing.T) {
	s, p, now := openTest(t)
	due := now.Add(time.Hour)
	_ = submitTest(t, s, now, due, 0, "", 3)
	s.Close()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), `"state":"scheduled"`, `"state":"ready"`, 1))
	if err = os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(p, DefaultPayloadLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	l, _, _, _, err := s2.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, due.Add(-time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if l != nil {
		t.Fatal("corrupt future-ready job executed early")
	}
	l, _, _, _, err = s2.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, due)
	if err != nil || l == nil {
		t.Fatalf("job not executable at due boundary: %v", err)
	}
}

func TestCompletionReplaySurvivesRestart(t *testing.T) {
	s, p, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	r := CompleteRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Result: json.RawMessage(`1`)}
	if _, idem, err := s.Complete(r, now); err != nil || idem {
		t.Fatalf("first completion err=%v idem=%v", err, idem)
	}
	s.Close()
	s2, err := Open(p, DefaultPayloadLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, idem, err := s2.Complete(r, now)
	if err != nil || !idem || got.State != model.Succeeded {
		t.Fatalf("restart replay err=%v idem=%v job=%#v", err, idem, got)
	}
}

func TestBatchRequeueIsAtomic(t *testing.T) {
	s, _, now := openTest(t)
	a := submitTest(t, s, now, now, 0, "", 3)
	b := submitTest(t, s, now, now, 0, "", 3)
	if _, err := s.Cancel(a.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(b.ID, now); err != nil {
		t.Fatal(err)
	}
	rows, err := s.RequeueMany([]string{a.ID, b.ID}, now)
	if err != nil || len(rows) != 2 {
		t.Fatalf("batch requeue: %v %#v", err, rows)
	}
	if _, err = s.Cancel(a.ID, now); err != nil {
		t.Fatal(err)
	}
	// b is ready, so the mixed selection must fail without requeueing a.
	if _, err = s.RequeueMany([]string{a.ID, b.ID}, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	ga, _ := s.Get(a.ID, now)
	if ga.State != model.Cancelled {
		t.Fatalf("batch was not atomic; a=%s", ga.State)
	}
}

func TestSucceededCannotBeRequeued(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 1)
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	if _, _, err := s.Complete(CompleteRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Requeue(j.ID, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("succeeded requeue accepted: %v", err)
	}
}

func TestPurgeThresholdIsStrict(t *testing.T) {
	s, _, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 1)
	if _, err := s.Cancel(j.ID, now); err != nil {
		t.Fatal(err)
	}
	n, err := s.Purge(now, 10, now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("purged at equal threshold")
	}
	n, err = s.Purge(now.Add(time.Nanosecond), 10, now)
	if err != nil || n != 1 {
		t.Fatalf("expected purge after strict threshold n=%d err=%v", n, err)
	}
}

func TestPaginationStableDuringStateTransitions(t *testing.T) {
	s, _, now := openTest(t)
	var ids []string
	for i := 0; i < 30; i++ {
		ids = append(ids, submitTest(t, s, now, now, 0, "", 3).ID)
	}
	p, err := s.List(ListFilter{Limit: 7}, now)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, j := range p.Jobs {
		seen[j.ID] = true
	}
	for _, id := range ids[10:20] {
		_, _ = s.Cancel(id, now)
	}
	cur := p.NextCursor
	for cur != "" {
		p, err = s.List(ListFilter{Limit: 7, Cursor: cur}, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range p.Jobs {
			if seen[j.ID] {
				t.Fatalf("duplicate %s", j.ID)
			}
			seen[j.ID] = true
		}
		cur = p.NextCursor
	}
	if len(seen) != 30 {
		t.Fatalf("state transitions caused skip: got %d", len(seen))
	}
}

func TestLeaseDurationEdges(t *testing.T) {
	s, _, now := openTest(t)
	_ = submitTest(t, s, now, now, 0, "", 3)
	if _, _, _, _, err := s.Claim(ClaimRequest{Worker: "w", Lease: 0}, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero lease not rejected: %v", err)
	}
	l, _, _, _, err := s.Claim(ClaimRequest{Worker: "w", Lease: 24 * time.Hour}, now)
	if err != nil || l == nil {
		t.Fatalf("max lease rejected: %v", err)
	}
}

func TestActiveLeaseRecoveryAcrossRestart(t *testing.T) {
	s, p, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "", 3)
	a, _, _, _, err := s.Claim(ClaimRequest{Worker: "a", Lease: 10 * time.Second}, now)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(p, DefaultPayloadLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	b, _, _, _, err := s2.Claim(ClaimRequest{Worker: "b", Lease: time.Minute}, now.Add(9*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Fatal("active persisted lease was double-claimed after restart")
	}
	// Expiry at +10s and fixed retry delay 1s means eligibility at +11s.
	b, _, _, _, err = s2.Claim(ClaimRequest{Worker: "b", Lease: time.Minute}, now.Add(11*time.Second))
	if err != nil || b == nil || b.Job.ID != j.ID || b.Job.AttemptCount != 2 {
		t.Fatalf("restart recovery failed: %#v %v", b, err)
	}
	if _, _, err = s2.Complete(CompleteRequest{JobID: j.ID, Worker: "a", Token: a.Token, Generation: a.Job.LeaseGeneration, Result: json.RawMessage(`1`)}, now.Add(11*time.Second)); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("pre-restart stale worker accepted: %v", err)
	}
}

func TestReadinessFailsWhenStorageDirectoryUnavailable(t *testing.T) {
	s, _, _ := openTest(t)
	old := s.path
	s.path = filepath.Join(t.TempDir(), "gone", "state.json")
	if err := s.Ready(); err == nil {
		t.Fatal("readiness stayed green with unavailable storage directory")
	}
	s.path = old
	if err := s.Ready(); err != nil {
		t.Fatalf("readiness did not recover: %v", err)
	}
}

func TestPermanentFailureRequiresPersistedPolicyPermission(t *testing.T) {
	s, _, now := openTest(t)
	j, _, err := s.Submit(SubmitRequest{Queue: "q", ScheduledAt: now, Payload: json.RawMessage(`1`), MaxAttempts: 2, RetryPolicy: model.RetryPolicy{Kind: model.RetryFixed, BaseDelayNS: int64(time.Second), AllowPermanentFailure: false}}, now)
	if err != nil {
		t.Fatal(err)
	}
	l, _, _, _, _ := s.Claim(ClaimRequest{Worker: "w", Lease: time.Minute}, now)
	got, _, retry, err := s.Fail(FailRequest{JobID: j.ID, Worker: "w", Token: l.Token, Generation: l.Job.LeaseGeneration, Error: "perm requested", Permanent: true}, now)
	if err != nil || !retry || got.State == model.Failed {
		t.Fatalf("worker bypassed retry policy: %#v retry=%v err=%v", got, retry, err)
	}
}

func TestIdleReapAndEmptyClaimDoNotRewriteSnapshot(t *testing.T) {
	s, p, now := openTest(t)
	before, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, _, _, err = s.Reap(now); err != nil {
		t.Fatal(err)
	}
	if l, _, _, _, err := s.Claim(ClaimRequest{Worker: "idle", Lease: time.Second}, now); err != nil || l != nil {
		t.Fatalf("empty claim l=%v err=%v", l, err)
	}
	after, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("no-op maintenance rewrote snapshot: before=%s after=%s", before.ModTime(), after.ModTime())
	}
}

func TestForgedFutureCursorRejected(t *testing.T) {
	s, _, now := openTest(t)
	_ = submitTest(t, s, now, now, 0, "", 3)
	if _, err := s.List(ListFilter{Limit: 10, Cursor: encodeCursor(cursor{SnapshotSeq: 999})}, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("future snapshot cursor accepted: %v", err)
	}
	if _, err := s.List(ListFilter{Limit: 10, Cursor: encodeCursor(cursor{AfterSeq: 2, SnapshotSeq: 1})}, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inverted cursor accepted: %v", err)
	}
}

func TestOpenAutoUpgradesLegacyV1(t *testing.T) {
	s, p, now := openTest(t)
	j := submitTest(t, s, now, now, 0, "auto-upgrade-key", 3)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(p, 1); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadSchemaVersion(p); err != nil || got != 1 {
		t.Fatalf("legacy version got %d want 1 err=%v", got, err)
	}

	s2, err := Open(p, DefaultPayloadLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, err := ReadSchemaVersion(p); err != nil || got != CurrentSchema {
		t.Fatalf("runtime auto-upgrade got %d want %d err=%v", got, CurrentSchema, err)
	}
	got, dup, err := s2.Submit(SubmitRequest{
		Queue:          "q",
		ScheduledAt:    now,
		Payload:        json.RawMessage(`{"changed":true}`),
		MaxAttempts:    3,
		RetryPolicy:    model.RetryPolicy{Kind: model.RetryFixed},
		IdempotencyKey: "auto-upgrade-key",
	}, now)
	if err != nil || !dup || got.ID != j.ID {
		t.Fatalf("auto-upgrade failed to reconstruct idempotency: %#v dup=%v err=%v", got, dup, err)
	}
}
