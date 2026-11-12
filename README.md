# Open Graph Parser for Golang

Go implementation of https://ogp.me/

[![reference](https://pkg.go.dev/badge/github.com/otiai10/opengraph)](https://pkg.go.dev/github.com/otiai10/opengraph/v2)
[![Go](https://github.com/otiai10/opengraph/workflows/Go/badge.svg)](https://github.com/otiai10/opengraph/actions)
[![codecov](https://codecov.io/gh/otiai10/opengraph/branch/main/graph/badge.svg?token=D4mPKqi9fH)](https://codecov.io/gh/otiai10/opengraph)
[![Maintainability](https://api.codeclimate.com/v1/badges/d063131639023e9a8900/maintainability)](https://codeclimate.com/github/otiai10/opengraph/maintainability)
[![Go Report Card](https://goreportcard.com/badge/github.com/otiai10/opengraph)](https://goreportcard.com/report/github.com/otiai10/opengraph)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/otiai10/opengraph/blob/main/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fotiai10%2Fopengraph.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fotiai10%2Fopengraph?ref=badge_shield)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/otiai10/opengraph?sort=semver)](https://pkg.go.dev/github.com/otiai10/opengraph/v2)

# Code Example

```go
package main

import (
	"fmt"
	"github.com/otiai10/opengraph/v2"
)

func main() {
	ogp, err := opengraph.Fetch("https://github.com/")
	fmt.Println(ogp, err)
}
```

# You can try CLI as a working example

```sh
% go install github.com/otiai10/opengraph/v2/cmd/ogp@latest
% ogp --help
% ogp -A otiai10.com
```

You can also use custom headers (useful for sites with bot protection):
```sh
% ogp -H "User-Agent: Mozilla/5.0" -H "Accept-Language: en-US" https://example.com
```

Just for fun 😉

# Advanced usage

Set an option for fetching:
```go
intent := opengraph.Intent{
	Context:     ctx,
	HTTPClient:  client,
	Strict:      true,
	TrustedTags: []string{"meta", "title"},
	Headers:     map[string]string{
		"User-Agent": "MyCustomBot/1.0",
		"Accept-Language": "en-US,en;q=0.9",
	},
}
ogp, err := opengraph.Fetch("https://ogp.me", intent)
```

## Concurrent atomic refresh

Atomic refresh is opt-in and off by default. Enable it on the object's
`Intent` by setting `AtomicRefresh` (or its alias `Atomic`), then call
`Fetch`, `Parse`, and/or `Walk` concurrently from multiple goroutines:

```go
ogp := opengraph.New("https://example.com")
ogp.Intent.AtomicRefresh = true

go ogp.Fetch()
go ogp.Parse(reader)
go ogp.Walk(node)

// Safe while refreshes are in flight.
snapshot := ogp.Snapshot()
fmt.Println(snapshot.Title)
```

Each successful call commits a complete page: fields missing from the new
page are cleared. Calls are ordered by start time, so an older call that
finishes late never overwrites a newer successful call. Failed calls (HTTP
errors, non-2xx responses, read or media-number failures, cancellation)
commit nothing. Atomic calls never mutate `Intent`.

Read exported fields directly only when no refresh is running; use `Snapshot`
for concurrent reads. Without atomic mode the original behavior and
zero-value semantics are unchanged. `Snapshot` returns a deep copy, so
mutating slices such as `Image` does not affect the object.

Use any `io.Reader` as a data source:

```go
f, _ := os.Open("my_test.html")
defer f.Close()
ogp := &opengraph.OpenGraph{}
err := ogp.Parse(f)
```

or if you already have parsed `*html.Node`:

```go
err := ogp.Walk(node)
```

Do you wanna make absolute URLs?:
```go
ogp.Image[0].URL // /logo.png
ogp.ToAbs()
ogp.Image[0].URL // https://ogp.me/logo.png
```

# Issues

- https://github.com/otiai10/opengraph/issues
