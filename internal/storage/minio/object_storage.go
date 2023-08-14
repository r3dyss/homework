package minio

import (
	"bytes"
	"context"
	"fmt"
	"github.com/spacelift-io/homework-object-storage/internal/core"
	"io"

	"github.com/minio/minio-go/v7"
)

type ObjectStorage struct {
	minioClient   *minio.Client
	defaultBucket string
}

const errKeyNoSuchKey = "NoSuchKey"

func NewObjectStorage(ctx context.Context, minioClient *minio.Client, defaultBucket string) (*ObjectStorage, error) {
	bucketExist, err := minioClient.BucketExists(ctx, defaultBucket)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket exists: %w", err)
	}

	if !bucketExist {
		if err := minioClient.MakeBucket(ctx, defaultBucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("creating bucket: %w", err)
		}
	}

	return &ObjectStorage{
		minioClient:   minioClient,
		defaultBucket: defaultBucket,
	}, nil
}

func (o *ObjectStorage) Put(ctx context.Context, objectID string, object []byte) error {
	_, err := o.minioClient.PutObject(ctx, o.defaultBucket, objectID, bytes.NewReader(object), int64(len(object)), minio.PutObjectOptions{})
	return err
}

func (o *ObjectStorage) Get(ctx context.Context, objectID string) ([]byte, error) {
	obj, err := o.minioClient.GetObject(ctx, o.defaultBucket, objectID, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	blob, err := io.ReadAll(obj)
	if err != nil {
		if minioErr, ok := err.(minio.ErrorResponse); ok {
			if minioErr.Code == errKeyNoSuchKey {
				return nil, core.ErrNotFound
			}
		}
		return nil, err
	}
	return blob, nil
}
