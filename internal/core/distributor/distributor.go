package distributor

import (
	"context"
	"fmt"
)

type ObjectDistributor struct {
	storageIDs []int
	storages   map[int]ObjectStorage
	selectorFn StorageSelectorFn
}

type ObjectStorage interface {
	Put(ctx context.Context, objectID string, blob []byte) error
	Get(ctx context.Context, objectID string) ([]byte, error)
}

type StorageSelectorFn func(ctx context.Context, objectID string, storageIDs []int) (int, error)

func NewObjectDistributor(storages map[int]ObjectStorage, selectorFn StorageSelectorFn) *ObjectDistributor {
	storageIDs := make([]int, 0)
	for k := range storages {
		storageIDs = append(storageIDs, k)
	}

	return &ObjectDistributor{
		storageIDs: storageIDs,
		storages:   storages,
		selectorFn: selectorFn,
	}
}

func (d *ObjectDistributor) PutObject(ctx context.Context, objectID string, blob []byte) error {
	objStorage, err := d.getObjectStorage(ctx, objectID)
	if err != nil {
		return err
	}

	return objStorage.Put(ctx, objectID, blob)
}

func (d *ObjectDistributor) GetObject(ctx context.Context, objectID string) ([]byte, error) {
	objStorage, err := d.getObjectStorage(ctx, objectID)
	if err != nil {
		return nil, err
	}

	return objStorage.Get(ctx, objectID)
}

func (d *ObjectDistributor) getObjectStorage(ctx context.Context, objectID string) (ObjectStorage, error) {
	storageID, err := d.selectorFn(ctx, objectID, d.storageIDs)
	if err != nil {
		return nil, fmt.Errorf("selecting storage ID: %w", err)
	}

	objStorage, ok := d.storages[storageID]
	if !ok {
		return nil, fmt.Errorf("selected '%d' storage does not exist", storageID)
	}

	return objStorage, nil
}
