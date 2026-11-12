package opengraph

import (
	"context"
	"net/http"
)

// Intent represents how to fetch, parse, and complete properties
// of this OpenGraph object.
// This SHOULD NOT have any meaning for "OpenGraph Protocol".
type Intent struct {

	// URL of this intent to fetch an OGP.
	// This does NOT mean `og:url` of the page.
	URL string

	// Context of the web request of this Intent.
	Context context.Context
	// HTTP Client to be used for this intent.
	HTTPClient *http.Client

	// Scrict is just an alias of `TrustedTags`.
	// `Strict == true` means `TrustedTags = ["meta"]`,
	// and `Strict == false` means `TrustedTags == ["meta", "title", "link"]`.
	Strict bool
	// TrustedTags specify which tags to be respected.
	TrustedTags []string

	// Headers to be sent with the HTTP request.
	// If nil, default browser-like headers will be used.
	Headers map[string]string

	// AtomicRefresh enables atomic refresh mode for concurrent calls to
	// Fetch, Parse, and Walk on the same OpenGraph object.
	//
	// In this mode each successful call publishes a complete page snapshot;
	// fields missing from the page are cleared. Calls are ordered by their
	// start time, so an older call that finishes late never overwrites a
	// newer successful call. Failed or cancelled calls never commit.
	//
	// The mode is off by default. Atomic refresh calls never modify Intent,
	// and direct reads of OpenGraph's exported fields are only race-free
	// when no call is in flight; use OpenGraph.Snapshot for concurrent reads.
	AtomicRefresh bool

	// Atomic is an alias for AtomicRefresh: setting either field enables
	// atomic refresh mode.
	Atomic bool
}
