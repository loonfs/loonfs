# LoonFS Go SDK

An idiomatic Go client for the LoonFS HTTP API. SDK v0.1.x targets LoonFS API
v0.3.x.

## Install

```sh
go get github.com/loonfs/loonfs-sdk-go@latest
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/loonfs/loonfs-sdk-go/client"
	"github.com/loonfs/loonfs-sdk-go/option"
)

func main() {
	loon := client.NewClient(
		option.WithBaseURL(os.Getenv("LOONFS_URL")),
		option.WithToken(os.Getenv("LOONFS_AUTH_TOKEN")),
	)

	capabilities, err := loon.System.GetCapabilities(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(capabilities.ProtocolVersion)
}
```

Upload and download helpers are available from the `transfers` package. See
[reference.md](./reference.md) for the generated API reference.

## Retries

The Go SDK makes one HTTP attempt by default. You can opt into retries with
`option.WithMaxAttempts`, but only do so for operations your application can
safely repeat.

## Generated code

This SDK is generated from the LoonFS OpenAPI specification. Please report SDK
issues in the [main LoonFS repository](https://github.com/loonfs/loonfs).

## License

Apache-2.0.
