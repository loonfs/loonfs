# LoonFS Go SDK

One module for LoonFS server and proxy applications. SDK v0.2.x targets LoonFS
API v0.3.x.

## Install

```sh
go get github.com/loonfs/loonfs-sdk-go@latest
```

Choose the package that matches where your code runs.

## Server

```go
package main

import (
	"context"
	"fmt"
	"os"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/files"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

func main() {
	loon := server.NewClient(
		option.WithBaseURL(os.Getenv("LOONFS_URL")),
		option.WithToken(os.Getenv("LOONFS_AUTH_TOKEN")),
		option.WithActorID(loonfs.String("example-user")),
	)

	capabilities, err := loon.Capabilities.Retrieve(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(capabilities.ProtocolVersion)

	commit, err := loon.Files.Upload(context.Background(), files.UploadInput{
		NamespaceID: "demo",
		Path:        "/hello.txt",
		Content:     []byte("hello"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(commit.CommitID, commit.Events)
}
```

Publishing helpers return the commit, including its events. Pass `CommitID`
explicitly if you may retry. Use `option.WithHTTPHeader` with `Loonfs-Actor`
to override the client default for a request.

`client.Files.DownloadStream(ctx, input)` opens a live, verified `io.ReadCloser`
in its `Content` field. Consume it through successful EOF to verify size and
checksum, and always close it. Closing early or cancelling the context releases
the response without claiming verification. A caller deadline covers metadata
and body reads; without one, the operation has a 60-second deadline. Direct and
proxied transfers use the configured HTTP client and the same context. Direct
requests carry only the presigned headers, and do not follow redirects.

`client.Files.Download` collects that stream into memory. `client.Files.Upload`
accepts an in-memory byte slice through the same transfer path.
`client.Files.PrepareStream(ctx, namespaceID, reader, sizeBytes)` consumes an
`io.Reader` once and returns prepared content for publication retries. Pass nil
for an unknown size. Small sources are prepared inline when advertised, up to the
smaller of the server limit and 64 KiB. Lookahead consumes at most that limit plus
one byte and preserves the prefix when continuing through an upload. Larger
unknown-size sources use multipart, retaining one provider-sized part plus the
lookahead prefix.

Preparation returns `files.PreparedFile`, either `*files.InlinePreparedContent`
(immutable bytes with no upload or expiry) or the existing `*files.PreparedContent`
(uploaded reference and token). Pass either to `UploadPrepared`. Use a type switch
before inspecting staged fields; existing staged struct literals remain supported.
`client.Files.UploadStream(ctx, files.StreamUploadInput{...})` prepares and
publishes in one operation. The context covers metadata, bytes and publication;
source and payload failures abort without replaying bytes. The caller owns and
closes the reader, including interrupting any blocking source read. See [reference.md](./reference.md) for the
generated API reference.

## Proxy

Use the `proxy` package in your backend to forward client requests while
keeping the LoonFS credential on the server.

Set `Authorize` to check each request and set `Loonfs-Actor` on forwarded
requests. The proxy always removes the browser's actor header.
Here, `authorizedActor` checks the application's session and namespace access.

```go
import (
	"net/http"
	"os"

	"github.com/loonfs/loonfs-sdk-go/proxy"
)

handler, err := proxy.NewHandler(proxy.Config{
	ServerBaseURL: os.Getenv("LOONFS_URL"),
	Token: os.Getenv("LOONFS_AUTH_TOKEN"),
	NamespaceAliases: map[string]string{"team-files": "demo"},
	Authorize: func(r *http.Request, route proxy.RouteContext) (proxy.Authorization, error) {
		actorID, ok := authorizedActor(r, route.NamespaceID)
		if !ok {
			return proxy.Authorization{}, &proxy.Refusal{Status: http.StatusForbidden}
		}
		return proxy.Authorization{ActorID: actorID}, nil
	},
})
if err != nil {
	panic(err)
}
http.Handle("/v0/", handler)
```

## Retries

The SDK retries responses that carry `Retry-After` and does not retry on
status alone. It never retries operations that LoonFS marks `not_idempotent`.
Use `option.WithMaxAttempts` to tune the attempt count.

For publication retries, call `client.Files.Prepare(ctx, namespaceID,
payload)` once and retain the returned `files.PreparedFile`. Publish it
with `client.Files.UploadPrepared(ctx, files.PreparedUploadInput{...})`, keeping
the prepared content, commit ID, path, actor, and options identical on every
attempt. Preparation does not create a visible file or extend the upload
lifetime. Calling `Upload` again prepares the source again: it may create a fresh
upload or select a different representation if capabilities changed. Retain the
prepared value for retries, including inline content, and never switch
representations after a failed or uncertain commit.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
