package objects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// PartSize is the multipart buffer for uploads of unknown length: the most
// memory one upload holds.
const PartSize = 16 << 20

// MinIO is the production Store.
type MinIO struct {
	c      *minio.Client
	bucket string
}

// MinIOConfig names the endpoint, bucket and the environment variables that
// hold the credentials.
type MinIOConfig struct {
	Endpoint     string
	Bucket       string
	UseSSL       bool
	AccessKeyEnv string
	SecretKeyEnv string
}

// NewMinIO builds a client. Credentials are read from the environment
// variables named in cfg.
func NewMinIO(cfg MinIOConfig) (*MinIO, error) {
	ak, sk := os.Getenv(cfg.AccessKeyEnv), os.Getenv(cfg.SecretKeyEnv)
	if ak == "" || sk == "" {
		return nil, fmt.Errorf("minio: %s and %s must be set", cfg.AccessKeyEnv, cfg.SecretKeyEnv)
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, ""), Secure: cfg.UseSSL})
	if err != nil {
		return nil, err
	}
	return &MinIO{c: c, bucket: cfg.Bucket}, nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if r := minio.ToErrorResponse(err); r.Code == "NoSuchKey" || r.Code == "NotFound" {
		return ErrNotFound
	}
	return err
}

func (m *MinIO) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := m.c.PutObject(ctx, m.bucket, key, r, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		PartSize:    PartSize,
	})
	return err
}

func (m *MinIO) Get(ctx context.Context, key string) (io.ReadCloser, Info, error) {
	obj, err := m.c.GetObject(ctx, m.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, Info{}, mapErr(err)
	}
	st, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, Info{}, mapErr(err)
	}
	return obj, Info{Key: key, Size: st.Size, LastModified: st.LastModified}, nil
}

func (m *MinIO) Stat(ctx context.Context, key string) (Info, error) {
	st, err := m.c.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return Info{}, mapErr(err)
	}
	return Info{Key: key, Size: st.Size, LastModified: st.LastModified}, nil
}

func (m *MinIO) Delete(ctx context.Context, key string) error {
	err := mapErr(m.c.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{}))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (m *MinIO) List(ctx context.Context, prefix string) ([]Info, error) {
	var out []Info
	for o := range m.c.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, Info{Key: o.Key, Size: o.Size, LastModified: o.LastModified})
	}
	return out, nil
}

func (m *MinIO) Copy(ctx context.Context, src, dst string) error {
	_, err := m.c.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: m.bucket, Object: dst},
		minio.CopySrcOptions{Bucket: m.bucket, Object: src})
	return mapErr(err)
}

func (m *MinIO) Ping(ctx context.Context) error {
	ok, err := m.c.BucketExists(ctx, m.bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("minio: bucket %s does not exist", m.bucket)
	}
	return nil
}
