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

	"github.com/loonfs/loonfs-sdk-go/server"
	"github.com/loonfs/loonfs-sdk-go/option"
)

func main() {
	loon := server.NewClient(
		option.WithBaseURL(os.Getenv("LOONFS_URL")),
		option.WithToken(os.Getenv("LOONFS_AUTH_TOKEN")),
	)

	capabilities, err := loon.Capabilities.Retrieve(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(capabilities.ProtocolVersion)
}
```

`client.Files.DownloadStream(ctx, input)` opens a live, verified `io.ReadCloser`
in its `Content` field. Consume it through successful EOF to verify size and
checksum, and always close it. Closing early or cancelling the context releases
the response without claiming verification. A caller deadline covers metadata
and body reads; without one, the operation has a 60-second deadline. Direct and
proxied transfers use the configured HTTP client and the same context. Direct
requests carry only the presigned headers, and do not follow redirects.

`client.Files.Download` collects that stream into memory. `client.Files.Upload`
accepts an in-memory byte slice. See [reference.md](./reference.md) for the
generated API reference.

## Proxy

Use the `proxy` package in your backend to forward client requests while
keeping the LoonFS credential on the server.

## Retries

The Go SDK makes one HTTP attempt by default. You can opt into retries with
`option.WithMaxAttempts`, but only do so for operations your application can
safely repeat.

For publication retries, call `client.Files.PrepareFileBytes(ctx, namespaceID,
payload)` once and retain the returned `*files.PreparedFileContent`. Publish it
with `client.Files.PutFilePrepared(ctx, files.PreparedUploadInput{...})`, keeping
the prepared content, commit ID, path, actor, and options identical on every
attempt. Preparation does not create a visible file or extend the upload
lifetime. Calling `Upload` again starts a fresh upload and cannot replay a
previously committed ID.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
