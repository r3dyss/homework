package distributor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFNVSelector(t *testing.T) {
	storageIDs := []int{1, 2}

	t.Run("given objectID, storageID will be selected", func(t *testing.T) {
		const objectID = "object_1"

		actualStorageID, err := FNVSelector(context.TODO(), objectID, storageIDs)
		require.NoError(t, err)

		assert.Equal(t, storageIDs[0], actualStorageID)

		t.Run("same objectID, return same storageID", func(t *testing.T) {
			actualStorageID, err := FNVSelector(context.TODO(), objectID, storageIDs)
			require.NoError(t, err)

			assert.Equal(t, storageIDs[0], actualStorageID)
		})

		t.Run("different objectID, will return different storageID", func(t *testing.T) {
			actualStorageID, err := FNVSelector(context.TODO(), "object_2", storageIDs)
			require.NoError(t, err)

			assert.Equal(t, storageIDs[1], actualStorageID)
		})
	})

	t.Run("when no storageIDs are given, error should be returned", func(t *testing.T) {
		_, err := FNVSelector(context.TODO(), "object_2", []int{})
		assert.Error(t, err)
	})
}
