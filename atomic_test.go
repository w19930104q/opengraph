package opengraph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	stdatomic "sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/html"
)

// atomicGate lets a test hold an HTTP response at a known point.
type atomicGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newAtomicGate() *atomicGate {
	return &atomicGate{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *atomicGate) signalStarted() {
	g.once.Do(func() { close(g.started) })
}

func (g *atomicGate) open() {
	close(g.release)
}

func waitStarted(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type atomicResponse struct {
	gate        *atomicGate
	status      int
	contentType string
	body        string
}

// newSequencedServer serves the responses in request order. Each response is
// held until its gate is opened, which makes completion order deterministic.
func newSequencedServer(t *testing.T, responses []atomicResponse) *httptest.Server {
	t.Helper()
	var requests stdatomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(requests.Add(1)) - 1
		if idx >= len(responses) {
			http.Error(w, "unexpected extra request", http.StatusInternalServerError)
			return
		}
		resp := responses[idx]
		resp.gate.signalStarted()
		select {
		case <-resp.gate.release:
		case <-r.Context().Done():
			return
		case <-time.After(10 * time.Second):
			return
		}
		w.Header().Set("Content-Type", resp.contentType)
		w.WriteHeader(resp.status)
		io.WriteString(w, resp.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func atomicPage(title, description, image string) string {
	return fmt.Sprintf(`<html><head>`+
		`<meta property="og:title" content="%s">`+
		`<meta property="og:description" content="%s">`+
		`<meta property="og:image" content="%s">`+
		`</head></html>`, title, description, image)
}

func mustParseNode(t *testing.T, page string) *html.Node {
	t.Helper()
	node, err := html.Parse(strings.NewReader(page))
	if err != nil {
		t.Fatalf("failed to parse test HTML: %v", err)
	}
	return node
}

func TestAtomicRefresh_DefaultOff(t *testing.T) {
	og := New("https://example.com")
	if og.Intent.AtomicRefresh || og.Intent.Atomic {
		t.Fatal("atomic refresh must be disabled on zero-value Intent")
	}
}

// Intent.Atomic is accepted as an alias for Intent.AtomicRefresh.
func TestAtomicRefresh_AtomicAlias(t *testing.T) {
	og := New("")
	og.Intent.Atomic = true
	if err := og.Parse(strings.NewReader(atomicPage("alias", "alias desc", "alias.png"))); err != nil {
		t.Fatalf("parse with Atomic alias failed: %v", err)
	}
	if snap := og.Snapshot(); snap.Title != "alias" {
		t.Fatalf("Atomic alias did not enable atomic refresh: %+v", snap)
	}
}

// A call that started later and committed successfully must not be
// overwritten by an older call that finishes afterwards. Waiting for the
// second request while the first is still blocked also proves that refreshes
// do not serialize their I/O.
func TestAtomicRefresh_NewSuccessThenOlderLate(t *testing.T) {
	gateOld, gateNew := newAtomicGate(), newAtomicGate()
	srv := newSequencedServer(t, []atomicResponse{
		{gate: gateOld, status: 200, contentType: "text/html", body: atomicPage("old", "old desc", "old.png")},
		{gate: gateNew, status: 200, contentType: "text/html", body: atomicPage("new", "new desc", "new.png")},
	})

	og := New(srv.URL)
	og.Intent.AtomicRefresh = true

	oldDone := make(chan error, 1)
	go func() { oldDone <- og.Fetch() }()
	waitStarted(t, gateOld.started, "old fetch")

	newDone := make(chan error, 1)
	go func() { newDone <- og.Fetch() }()
	// If refresh I/O were serialized, this request could never reach the
	// handler while the old one is still gated.
	waitStarted(t, gateNew.started, "new fetch")

	gateNew.open()
	if err := <-newDone; err != nil {
		t.Fatalf("new fetch failed: %v", err)
	}
	snapNew := og.Snapshot()
	if snapNew.Title != "new" || snapNew.Description != "new desc" || len(snapNew.Image) != 1 || snapNew.Image[0].URL != "new.png" {
		t.Fatalf("new commit did not publish complete snapshot: %+v", snapNew)
	}

	gateOld.open()
	if err := <-oldDone; err != nil {
		t.Fatalf("old fetch failed: %v", err)
	}
	snapAfter := og.Snapshot()
	if !reflect.DeepEqual(snapAfter, snapNew) {
		t.Fatalf("late older call overwrote newer commit:\nnew:   %+v\nafter: %+v", snapNew, snapAfter)
	}
}

// A newer call that fails must neither publish anything nor prevent an older
// successful call from committing.
func TestAtomicRefresh_NewFailureDoesNotBlockOldSuccess(t *testing.T) {
	gateOld, gateNew := newAtomicGate(), newAtomicGate()
	srv := newSequencedServer(t, []atomicResponse{
		{gate: gateOld, status: 200, contentType: "text/html", body: atomicPage("old", "old desc", "old.png")},
		{gate: gateNew, status: 503, contentType: "text/html", body: atomicPage("failed", "failed desc", "failed.png")},
	})

	og := New(srv.URL)
	og.Intent.AtomicRefresh = true

	oldDone := make(chan error, 1)
	go func() { oldDone <- og.Fetch() }()
	waitStarted(t, gateOld.started, "old fetch")

	newDone := make(chan error, 1)
	go func() { newDone <- og.Fetch() }()
	waitStarted(t, gateNew.started, "new fetch")

	gateNew.open()
	errNew := <-newDone
	if errNew == nil {
		t.Fatal("expected the 503 response to fail")
	}
	if !strings.Contains(errNew.Error(), "503") {
		t.Fatalf("status error should be traceable, got %v", errNew)
	}
	if snap := og.Snapshot(); snap.Title != "" || len(snap.Image) != 0 {
		t.Fatalf("failed newer call must not commit: %+v", snap)
	}

	gateOld.open()
	if err := <-oldDone; err != nil {
		t.Fatalf("older call should still succeed: %v", err)
	}
	snap := og.Snapshot()
	if snap.Title != "old" || snap.Description != "old desc" || len(snap.Image) != 1 {
		t.Fatalf("older call did not commit after newer failure: %+v", snap)
	}
}

// Non-2xx responses and malformed nested media numbers must return an error,
// leave the previous snapshot intact, and expose the underlying error.
func TestAtomicRefresh_StatusAndNestedNumberFailuresDoNotCommit(t *testing.T) {
	gate := newAtomicGate()
	srv := newSequencedServer(t, []atomicResponse{
		{gate: gate, status: 500, contentType: "text/html", body: atomicPage("bad", "bad desc", "bad.png")},
	})

	og := New("")
	og.Intent.AtomicRefresh = true
	og.Intent.URL = srv.URL
	if err := og.Parse(strings.NewReader(atomicPage("good", "good desc", "good.png"))); err != nil {
		t.Fatalf("initial parse failed: %v", err)
	}
	before := og.Snapshot()

	gate.open()
	err := og.Fetch()
	if err == nil {
		t.Fatal("expected non-2xx fetch to fail")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("non-2xx error is not traceable: %v", err)
	}
	if after := og.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("non-2xx response committed data:\nbefore: %+v\nafter:  %+v", before, after)
	}

	badNumber := `<html><body><section><div>` +
		`<meta property="og:title" content="bad">` +
		`<meta property="og:image" content="bad.png">` +
		`<meta property="og:image:width" content="not-a-number">` +
		`</div></section></body></html>`
	err = og.Parse(strings.NewReader(badNumber))
	if err == nil {
		t.Fatal("expected malformed media number to fail")
	}
	var numErr *strconv.NumError
	if !errors.As(err, &numErr) {
		t.Fatalf("*strconv.NumError not traceable in %v", err)
	}
	if numErr.Num != "not-a-number" {
		t.Fatalf("unexpected NumError: %+v", numErr)
	}
	if !errors.Is(err, strconv.ErrSyntax) {
		t.Fatalf("strconv.ErrSyntax not traceable in %v", err)
	}
	if after := og.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("nested contribution failure committed partial data:\nbefore: %+v\nafter:  %+v", before, after)
	}
}

// A context cancellation while the body is being read must return a
// traceable cancellation error and must not commit.
func TestAtomicRefresh_CancelDuringReadDoesNotCommit(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `<html><head><meta property="og:title" content="partial">`)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	og := New("")
	og.Intent.AtomicRefresh = true
	og.Intent.Context = ctx
	og.Intent.URL = srv.URL
	if err := og.Parse(strings.NewReader(atomicPage("committed", "committed desc", "committed.png"))); err != nil {
		t.Fatalf("initial parse failed: %v", err)
	}
	before := og.Snapshot()

	done := make(chan error, 1)
	go func() { done <- og.Fetch() }()
	waitStarted(t, started, "streaming response")

	cancel()
	err := <-done
	if err == nil {
		t.Fatal("expected cancelled fetch to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation is not traceable in %v", err)
	}
	if after := og.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("cancelled read committed data:\nbefore: %+v\nafter:  %+v", before, after)
	}
}

// A plain reader failure must also be returned with its cause intact and
// must not commit.
func TestAtomicRefresh_ReadFailureDoesNotCommit(t *testing.T) {
	sentinel := errors.New("sentinel read failure")
	r := io.MultiReader(
		strings.NewReader(`<html><head><meta property="og:title" content="partial">`),
		errorReader{err: sentinel},
	)

	og := New("")
	og.Intent.AtomicRefresh = true
	if err := og.Parse(strings.NewReader(atomicPage("committed", "committed desc", "committed.png"))); err != nil {
		t.Fatalf("initial parse failed: %v", err)
	}
	before := og.Snapshot()

	err := og.Parse(r)
	if err == nil {
		t.Fatal("expected read failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("underlying read error is not traceable in %v", err)
	}
	if after := og.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("read failure committed data:\nbefore: %+v\nafter:  %+v", before, after)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

// Snapshot must be a deep copy, including every media slice and the Intent's
// reference fields.
func TestAtomicRefresh_SnapshotIsolation(t *testing.T) {
	og := New("")
	og.Intent.AtomicRefresh = true
	og.Intent.Headers = map[string]string{"X-Test": "original"}
	og.Intent.TrustedTags = []string{HTMLMetaTag, HTMLTitleTag, HTMLLinkTag}

	page := `<html><head>` +
		`<meta property="og:title" content="t">` +
		`<meta property="og:image" content="a.png">` +
		`<meta property="og:image" content="b.png">` +
		`<meta property="og:audio" content="a.mp3">` +
		`<meta property="og:video" content="v.mp4">` +
		`<link rel="icon" href="i.png">` +
		`<meta property="og:site_name" content="site">` +
		`</head></html>`
	if err := og.Parse(strings.NewReader(page)); err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	original := og.Snapshot()
	snap := og.Snapshot()

	snap.Title = "mutated"
	snap.Image[0].URL = "mutated.png"
	snap.Image = append(snap.Image, Image{URL: "extra.png"})
	snap.Audio[0].URL = "mutated.mp3"
	snap.Video[0].Type = "mutated/video"
	snap.Favicon.URL = "mutated-icon.png"
	snap.LocaleAlt = append(snap.LocaleAlt, "mutated")
	snap.Intent.Headers["X-Test"] = "mutated"
	snap.Intent.TrustedTags[0] = "mutated-tag"

	after := og.Snapshot()
	if !reflect.DeepEqual(after, original) {
		t.Fatalf("mutation of Snapshot leaked into the object:\noriginal: %+v\nafter:    %+v", original, after)
	}
}

// Every successful commit is a whole-page snapshot: fields absent from the
// new page are cleared.
func TestAtomicRefresh_CommitClearsMissingFields(t *testing.T) {
	og := New("")
	og.Intent.AtomicRefresh = true

	rich := `<html><head>` +
		`<meta property="og:title" content="one">` +
		`<meta property="og:description" content="desc">` +
		`<meta property="og:type" content="article">` +
		`<meta property="og:site_name" content="site">` +
		`<meta property="og:image" content="one.png">` +
		`<meta property="og:audio" content="one.mp3">` +
		`<meta property="og:video" content="one.mp4">` +
		`<link rel="icon" href="one.ico">` +
		`</head></html>`
	if err := og.Parse(strings.NewReader(rich)); err != nil {
		t.Fatalf("rich parse failed: %v", err)
	}
	first := og.Snapshot()
	if first.Title != "one" || len(first.Image) != 1 || len(first.Audio) != 1 ||
		len(first.Video) != 1 || first.Favicon.URL != "one.ico" {
		t.Fatalf("unexpected first snapshot: %+v", first)
	}

	sparse := `<html><head><meta property="og:title" content="two"></head></html>`
	if err := og.Parse(strings.NewReader(sparse)); err != nil {
		t.Fatalf("sparse parse failed: %v", err)
	}
	second := og.Snapshot()
	if second.Title != "two" {
		t.Fatalf("expected new title, got %+v", second)
	}
	if second.Description != "" || second.Type != "" || second.SiteName != "" ||
		len(second.Image) != 0 || len(second.Audio) != 0 || len(second.Video) != 0 ||
		second.Favicon.URL != "" {
		t.Fatalf("missing fields were not cleared: %+v", second)
	}
}

// Strict mode, custom TrustedTags, the zero-value object, and the favicon
// completion rule keep their legacy semantics in atomic mode.
func TestAtomicRefresh_PreservesLegacyOptions(t *testing.T) {
	var zero OpenGraph
	zero.Intent.AtomicRefresh = true
	beforeIntent := cloneIntent(zero.Intent)

	if err := zero.Parse(strings.NewReader(atomicPage("zero", "zero desc", "zero.png"))); err != nil {
		t.Fatalf("zero-value parse failed: %v", err)
	}
	if snap := zero.Snapshot(); snap.Title != "zero" || len(snap.Image) != 1 {
		t.Fatalf("zero-value object did not commit: %+v", snap)
	}
	if !reflect.DeepEqual(beforeIntent, zero.Intent) {
		t.Fatalf("atomic Parse modified Intent: before %+v after %+v", beforeIntent, zero.Intent)
	}

	// TrustedTags and Strict are read from Intent copies without being written.
	node := mustParseNode(t, `<html><head>`+
		`<title>Title Tag</title>`+
		`<meta name="description" content="name description">`+
		`<meta property="og:title" content="OG Title">`+
		`</head></html>`)

	og := New("")
	og.Intent.AtomicRefresh = true
	og.Intent.Strict = true
	if err := og.Walk(node); err != nil {
		t.Fatalf("strict walk failed: %v", err)
	}
	if snap := og.Snapshot(); snap.Title != "OG Title" || snap.Description != "" {
		t.Fatalf("strict mode changed: %+v", snap)
	}
	if og.Intent.TrustedTags != nil {
		t.Fatalf("atomic Walk wrote Intent.TrustedTags: %v", og.Intent.TrustedTags)
	}

	og.Intent.Strict = false
	og.Intent.TrustedTags = []string{HTMLTitleTag}
	if err := og.Walk(node); err != nil {
		t.Fatalf("custom TrustedTags walk failed: %v", err)
	}
	if snap := og.Snapshot(); snap.Title != "Title Tag" {
		t.Fatalf("custom TrustedTags were not honored: %+v", snap)
	}

	// The default favicon applies to non-strict Fetch, but not strict Fetch.
	titleServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, `<html><head><title>no icon</title></head></html>`)
	}))
	t.Cleanup(titleServer.Close)

	nonStrict := New(titleServer.URL)
	nonStrict.Intent.AtomicRefresh = true
	if err := nonStrict.Fetch(); err != nil {
		t.Fatalf("non-strict fetch failed: %v", err)
	}
	if snap := nonStrict.Snapshot(); snap.Title != "no icon" || snap.Favicon.URL != "/favicon.ico" {
		t.Fatalf("favicon completion rule changed for non-strict mode: %+v", snap)
	}
	if nonStrict.Intent.HTTPClient != nil || nonStrict.Intent.Context != nil || nonStrict.Intent.TrustedTags != nil {
		t.Fatalf("atomic Fetch modified Intent defaults: %+v", nonStrict.Intent)
	}

	strict := New(titleServer.URL)
	strict.Intent.AtomicRefresh = true
	strict.Intent.Strict = true
	if err := strict.Fetch(); err != nil {
		t.Fatalf("strict fetch failed: %v", err)
	}
	if snap := strict.Snapshot(); snap.Title != "" || snap.Favicon.URL != "" {
		t.Fatalf("strict fetch should ignore title/favicon completion: %+v", snap)
	}
}

// All three entry points share one sequence space: a slow older Fetch must
// lose to Walk and Parse calls that started later and committed.
func TestAtomicRefresh_MixedEntrypointsOrdering(t *testing.T) {
	gateOld := newAtomicGate()
	srv := newSequencedServer(t, []atomicResponse{
		{gate: gateOld, status: 200, contentType: "text/html", body: atomicPage("fetch-old", "fetch desc", "fetch.png")},
	})

	og := New(srv.URL)
	og.Intent.AtomicRefresh = true

	oldDone := make(chan error, 1)
	go func() { oldDone <- og.Fetch() }()
	waitStarted(t, gateOld.started, "old fetch")

	walkNode := mustParseNode(t, atomicPage("walk", "walk desc", "walk.png"))
	if err := og.Walk(walkNode); err != nil {
		t.Fatalf("atomic Walk failed: %v", err)
	}
	if snap := og.Snapshot(); snap.Title != "walk" {
		t.Fatalf("Walk did not commit: %+v", snap)
	}

	if err := og.Parse(strings.NewReader(atomicPage("parse", "parse desc", "parse.png"))); err != nil {
		t.Fatalf("atomic Parse failed: %v", err)
	}
	snapParse := og.Snapshot()
	if snapParse.Title != "parse" || snapParse.Description != "parse desc" {
		t.Fatalf("Parse did not commit: %+v", snapParse)
	}

	gateOld.open()
	if err := <-oldDone; err != nil {
		t.Fatalf("old fetch failed: %v", err)
	}
	if after := og.Snapshot(); !reflect.DeepEqual(after, snapParse) {
		t.Fatalf("late Fetch overwrote a newer mixed-entrypoint commit:\nnew:   %+v\nafter: %+v", snapParse, after)
	}
}

// Concurrent refreshes through all entry points, with concurrent snapshots,
// must never expose a torn page or deadlock. Run with -race.
func TestAtomicRefresh_ConcurrentMixedEntrypointsConsistent(t *testing.T) {
	pages := make([]string, 16)
	for i := range pages {
		pages[i] = atomicPage(
			fmt.Sprintf("title-%d", i),
			fmt.Sprintf("desc-%d", i),
			fmt.Sprintf("https://example.com/%d.png", i),
		)
	}

	var requests stdatomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(requests.Add(1)-1) % len(pages)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, pages[i])
	}))
	t.Cleanup(srv.Close)

	nodes := make([]*html.Node, len(pages))
	for i, page := range pages {
		nodes[i] = mustParseNode(t, page)
	}

	og := New(srv.URL)
	og.Intent.AtomicRefresh = true

	var workersWG sync.WaitGroup
	errCh := make(chan error, 256)
	stopSnapshots := make(chan struct{})
	snapshotDone := make(chan struct{})

	go func() {
		defer close(snapshotDone)
		for {
			select {
			case <-stopSnapshots:
				return
			default:
			}
			snap := og.Snapshot()
			if snap.Title == "" {
				continue // no commit yet
			}
			var i int
			if _, err := fmt.Sscanf(snap.Title, "title-%d", &i); err != nil {
				errCh <- fmt.Errorf("torn title %q: %v", snap.Title, err)
				return
			}
			if snap.Description != fmt.Sprintf("desc-%d", i) ||
				len(snap.Image) != 1 ||
				snap.Image[0].URL != fmt.Sprintf("https://example.com/%d.png", i) {
				errCh <- fmt.Errorf("torn snapshot for page %d: %+v", i, snap)
				return
			}
		}
	}()

	const workers = 12
	const iterations = 15
	for w := 0; w < workers; w++ {
		w := w
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for n := 0; n < iterations; n++ {
				switch (w + n) % 3 {
				case 0:
					if err := og.Fetch(); err != nil {
						errCh <- fmt.Errorf("fetch: %w", err)
						return
					}
				case 1:
					if err := og.Parse(strings.NewReader(pages[(w+n)%len(pages)])); err != nil {
						errCh <- fmt.Errorf("parse: %w", err)
						return
					}
				case 2:
					if err := og.Walk(nodes[(w+n)%len(nodes)]); err != nil {
						errCh <- fmt.Errorf("walk: %w", err)
						return
					}
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { workersWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("possible deadlock: concurrent refreshes did not finish")
	}
	close(stopSnapshots)
	<-snapshotDone

	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	final := og.Snapshot()
	if final.Title == "" {
		t.Fatalf("no successful commit reached the object: %+v", final)
	}
	var i int
	if _, err := fmt.Sscanf(final.Title, "title-%d", &i); err != nil {
		t.Fatalf("unexpected final title %q: %v", final.Title, err)
	}
	if final.Description != fmt.Sprintf("desc-%d", i) ||
		len(final.Image) != 1 ||
		final.Image[0].URL != fmt.Sprintf("https://example.com/%d.png", i) {
		t.Fatalf("final snapshot is not a complete page: %+v", final)
	}
}
