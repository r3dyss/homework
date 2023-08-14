package minio

import (
	"context"
	"testing"

	"github.com/spacelift-io/homework-object-storage/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectStorage(t *testing.T) {
	storage, err := NewObjectStorage(context.Background(), testEnvironment.minioClient)
	require.NoError(t, err)

	t.Run("object should be put to storage", func(t *testing.T) {
		const objectID = "object_1"

		blob := []byte("blob")
		err := storage.Put(context.Background(), objectID, blob)
		require.NoError(t, err)

		actualBlob, err := storage.Get(context.Background(), objectID)
		assert.NoError(t, err)

		assert.Equal(t, blob, actualBlob)

		t.Run("object with same ID, should override", func(t *testing.T) {
			blob2 := []byte("blob_2")
			err := storage.Put(context.Background(), objectID, blob2)
			require.NoError(t, err)

			actualBlob, err := storage.Get(context.Background(), objectID)
			assert.NoError(t, err)

			assert.Equal(t, blob2, actualBlob)
		})
	})

	t.Run("object does not exist, should return not found", func(t *testing.T) {
		_, err := storage.Get(context.Background(), "random_object_key")
		assert.Equal(t, core.ErrNotFound, err)
	})
}
