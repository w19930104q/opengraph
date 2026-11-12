package opengraph

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"golang.org/x/net/html"
)

// Atomic refresh mode
//
// When Intent.AtomicRefresh is enabled, Fetch, Parse, and Walk become
// atomic refresh operations on an OpenGraph object: each call parses a
// complete page into private staging storage and, only if it succeeds,
// publishes that staging storage as a whole snapshot under a short lock.
// Network and parser I/O never happens while the lock is held, so refresh
// calls on the same object can run concurrently.
//
// Calls reserve a monotonically increasing sequence number when they start.
// A successful call may commit only if no call that started later has
// already committed; a late-finishing older call is therefore discarded.
// Failed calls (HTTP errors, non-2xx responses, read errors, malformed
// media numbers, cancellation) never commit and never prevent another
// call from committing.
//
// Atomic mode never writes to Intent. The zero-value OpenGraph remains
// usable, and when the mode is disabled all legacy behavior is unchanged.

// atomicRefreshEnabled reports whether the current Intent requests atomic
// refresh mode. Atomic and AtomicRefresh are aliases.
func (og *OpenGraph) atomicRefreshEnabled() bool {
	return og.Intent.AtomicRefresh || og.Intent.Atomic
}

// beginRefresh reserves a sequence number in call-start order.
func (og *OpenGraph) beginRefresh() uint64 {
	og.refreshMu.Lock()
	defer og.refreshMu.Unlock()
	og.refreshNext++
	return og.refreshNext
}

// commit atomically publishes src as the complete metadata snapshot for og,
// unless a call that started later has already committed successfully.
func (og *OpenGraph) commit(seq uint64, src *OpenGraph) {
	og.refreshMu.Lock()
	defer og.refreshMu.Unlock()
	if seq < og.refreshCommitted {
		return
	}

	og.Title = src.Title
	og.Type = src.Type
	og.Image = src.Image
	og.URL = src.URL
	og.Audio = src.Audio
	og.Description = src.Description
	og.Determiner = src.Determiner
	og.Locale = src.Locale
	og.LocaleAlt = src.LocaleAlt
	og.SiteName = src.SiteName
	og.Video = src.Video
	og.Favicon = src.Favicon

	og.refreshCommitted = seq
}

// staging returns a fresh, empty OpenGraph for a refresh call. Intent is
// copied so that the shared object's Intent is never modified; the copy is
// only used to read parsing options.
func (og *OpenGraph) staging(intent Intent) *OpenGraph {
	dst := New("")
	dst.Intent = intent
	return dst
}

// parseHTMLInto parses body into dst and reports all nested contribution
// errors, wrapping the underlying error so errors.Is and errors.As remain
// usable by callers.
func parseHTMLInto(dst *OpenGraph, body io.Reader) error {
	node, err := html.Parse(body)
	if err != nil {
		return fmt.Errorf("opengraph: read/parse HTML: %w", err)
	}
	return dst.walkTree(node)
}

// walkTree is the strict-error variant of Walk used by atomic refreshes.
func (og *OpenGraph) walkTree(node *html.Node) error {
	if len(og.Intent.TrustedTags) == 0 {
		if og.Intent.Strict {
			og.Intent.TrustedTags = []string{HTMLMetaTag}
		} else {
			og.Intent.TrustedTags = []string{HTMLMetaTag, HTMLTitleTag, HTMLLinkTag}
		}
	}
	return og.walkTreeRecursive(node)
}

func (og *OpenGraph) walkTreeRecursive(node *html.Node) error {
	if node.Type == html.ElementNode {
		switch {
		case node.Data == HTMLMetaTag && og.trust(node.Data):
			if err := MetaTag(node).Contribute(og); err != nil {
				return fmt.Errorf("opengraph: contribute <%s>: %w", node.Data, err)
			}
			return nil
		case node.Data == HTMLTitleTag && og.trust(node.Data):
			if err := TitleTag(node).Contribute(og); err != nil {
				return fmt.Errorf("opengraph: contribute <%s>: %w", node.Data, err)
			}
			return nil
		case node.Data == HTMLLinkTag && og.trust(node.Data):
			if err := LinkTag(node).Contribute(og); err != nil {
				return fmt.Errorf("opengraph: contribute <%s>: %w", node.Data, err)
			}
			return nil
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if err := og.walkTreeRecursive(child); err != nil {
			return err
		}
	}
	return nil
}

// fetchAtomic implements Fetch for atomic refresh mode.
func (og *OpenGraph) fetchAtomic(seq uint64) error {
	// Read Intent once. This call never writes it, so concurrent refresh
	// calls only ever read the same configuration.
	intent := og.Intent

	if intent.URL == "" {
		return fmt.Errorf("no URL given yet")
	}

	client := intent.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	ctx := intent.Context
	if ctx == nil {
		ctx = context.Background()
	}

	req, err := http.NewRequest("GET", intent.URL, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	headers := intent.Headers
	if headers == nil {
		headers = map[string]string{
			"User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
			"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.9",
		}
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("opengraph: unexpected HTTP status: %s", res.Status)
	}

	if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/html") {
		return fmt.Errorf("Content type must be text/html")
	}

	staging := og.staging(intent)
	if err := parseHTMLInto(staging, res.Body); err != nil {
		return err
	}

	if !intent.Strict && staging.Favicon.URL == "" {
		staging.Favicon.URL = "/favicon.ico"
	}

	og.commit(seq, staging)
	return nil
}

// parseAtomic implements Parse for atomic refresh mode.
func (og *OpenGraph) parseAtomic(seq uint64, body io.Reader) error {
	intent := og.Intent

	staging := og.staging(intent)
	if err := parseHTMLInto(staging, body); err != nil {
		return err
	}

	og.commit(seq, staging)
	return nil
}

// walkAtomic implements Walk for atomic refresh mode.
func (og *OpenGraph) walkAtomic(seq uint64, node *html.Node) error {
	intent := og.Intent

	staging := og.staging(intent)
	if err := staging.walkTree(node); err != nil {
		return err
	}

	og.commit(seq, staging)
	return nil
}

// Snapshot returns a concurrency-safe, deep copy of the currently committed
// metadata. The returned value shares no slices or maps with og, so mutating
// it cannot affect the receiver.
//
// Snapshot is safe to call concurrently with atomic refresh calls. Direct
// reads of the exported fields are only safe when no refresh is in flight.
func (og *OpenGraph) Snapshot() *OpenGraph {
	og.refreshMu.RLock()
	defer og.refreshMu.RUnlock()

	return &OpenGraph{
		Title:       og.Title,
		Type:        og.Type,
		Image:       cloneImages(og.Image),
		URL:         og.URL,
		Audio:       cloneAudios(og.Audio),
		Description: og.Description,
		Determiner:  og.Determiner,
		Locale:      og.Locale,
		LocaleAlt:   cloneStrings(og.LocaleAlt),
		SiteName:    og.SiteName,
		Video:       cloneVideos(og.Video),
		Favicon:     og.Favicon,
		Intent:      cloneIntent(og.Intent),
	}
}

func cloneImages(src []Image) []Image {
	if src == nil {
		return nil
	}
	dst := make([]Image, len(src))
	copy(dst, src)
	return dst
}

func cloneAudios(src []Audio) []Audio {
	if src == nil {
		return nil
	}
	dst := make([]Audio, len(src))
	copy(dst, src)
	return dst
}

func cloneVideos(src []Video) []Video {
	if src == nil {
		return nil
	}
	dst := make([]Video, len(src))
	copy(dst, src)
	return dst
}

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneIntent(src Intent) Intent {
	dst := src
	dst.TrustedTags = cloneStrings(src.TrustedTags)
	if src.Headers != nil {
		dst.Headers = make(map[string]string, len(src.Headers))
		for key, value := range src.Headers {
			dst.Headers[key] = value
		}
	}
	return dst
}
