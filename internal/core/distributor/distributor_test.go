package distributor

import (
	"context"
	"github.com/spacelift-io/homework-object-storage/internal/core"
	"testing"

	"github.com/spacelift-io/homework-object-storage/internal/storage/memory"
	"github.com/stretchr/testify/assert"
)

func constSelector(storageID int) func(ctx context.Context, objectID string, storageIDs []int) (int, error) {
	return func(ctx context.Context, objectID string, storageIDs []int) (int, error) {
		return storageID, nil
	}
}

func TestObjectDistributor(t *testing.T) {
	t.Run("when given object, it is put in object storage", func(t *testing.T) {
		const objectID = "object_id"

		distributor := NewObjectDistributor(map[int]ObjectStorage{
			1: memory.NewObjectStorage(),
		}, constSelector(1))

		blob := []byte("Hello")

		err := distributor.PutObject(context.TODO(), objectID, blob)
		assert.NoError(t, err)

		actualObject, err := distributor.GetObject(context.TODO(), objectID)
		assert.NoError(t, err)
		assert.Equal(t, blob, actualObject)

		t.Run("when different object is given, object will be overwritten", func(t *testing.T) {
			blob := []byte("Hello second")

			err := distributor.PutObject(context.TODO(), objectID, blob)
			assert.NoError(t, err)

			actualObject, err := distributor.GetObject(context.TODO(), objectID)
			assert.NoError(t, err)
			assert.Equal(t, blob, actualObject)
		})
	})

	t.Run("when given 3 objects and 3 different storages, objects will be distributed evenly", func(t *testing.T) {
		objects := map[string][]byte{
			"object_1": []byte("object_1"),
			"object_2": []byte("object_2"),
			"object_3": []byte("object_3"),
		}

		memoryStorages := []*memory.ObjectStorage{
			memory.NewObjectStorage(),
			memory.NewObjectStorage(),
			memory.NewObjectStorage(),
		}

		distributor := NewObjectDistributor(map[int]ObjectStorage{
			1: memoryStorages[0],
			2: memoryStorages[1],
			3: memoryStorages[2],
		}, FNVSelector)

		for objID, obj := range objects {
			err := distributor.PutObject(context.TODO(), objID, obj)
			assert.NoError(t, err)

			actualObject, err := distributor.GetObject(context.TODO(), objID)
			assert.NoError(t, err)
			assert.Equal(t, obj, actualObject)
		}

		for _, objStorage := range memoryStorages {
			objLen := objStorage.ObjectCount()
			assert.Equal(t, 1, objLen)
		}
	})

	t.Run("when unknown objectID is given, we should return not found", func(t *testing.T) {
		distributor := NewObjectDistributor(map[int]ObjectStorage{
			1: memory.NewObjectStorage(),
		}, FNVSelector)
		_, err := distributor.GetObject(context.TODO(), "random_object_id")
		assert.Equal(t, core.ErrNotFound, err)
	})
}
