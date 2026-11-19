// Package objects is hourchd's object store: contexts, artifacts, logs and
// backups. hourchd is the only MinIO client. Every method streams; nothing
// buffers a whole object (MinIO multipart uploads use one PartSize buffer).
package objects

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned for a missing key.
var ErrNotFound = errors.New("object not found")

// Info describes one object.
type Info struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is the object store interface. Errors other than ErrNotFound mean the
// store could not be reached or failed (the API maps them to 503).
type Store interface {
	// Put streams r to key, replacing any object there. size is the exact
	// length, or -1 when unknown.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Get opens key for streaming.
	Get(ctx context.Context, key string) (io.ReadCloser, Info, error)
	Stat(ctx context.Context, key string) (Info, error)
	// Delete removes key; a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns every object under prefix (recursively), sorted by key.
	List(ctx context.Context, prefix string) ([]Info, error)
	// Copy copies src to dst server side.
	Copy(ctx context.Context, src, dst string) error
	// Ping checks the store is reachable.
	Ping(ctx context.Context) error
}
