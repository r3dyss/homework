package distributor

import (
	"context"
	"fmt"
	"sync"
)

type ObjectDistributor struct {
	storages        map[string]ObjectStorage
	storageSelector StorageSelector
	l               sync.RWMutex
}

type ObjectStorage interface {
	Put(ctx context.Context, objectID string, blob []byte) error
	Get(ctx context.Context, objectID string) ([]byte, error)
}

type StorageSelector interface {
	AddStorage(storageID string)
	RemoveStorage(storageID string)
	LocateStorage(objectID string) string
}

func NewObjectDistributor(storageSelector StorageSelector) *ObjectDistributor {
	return &ObjectDistributor{
		storages:        make(map[string]ObjectStorage),
		storageSelector: storageSelector,
	}
}

func (d *ObjectDistributor) AddStorage(storageID string, storage ObjectStorage) {
	d.l.Lock()
	defer d.l.Unlock()

	d.storages[storageID] = storage
	d.storageSelector.AddStorage(storageID)
}

func (d *ObjectDistributor) RemoveStorage(storageID string) {
	d.l.Lock()
	defer d.l.Unlock()

	delete(d.storages, storageID)
	d.storageSelector.RemoveStorage(storageID)
}

func (d *ObjectDistributor) PutObject(ctx context.Context, objectID string, blob []byte) error {
	objStorage, err := d.getObjectStorage(objectID)
	if err != nil {
		return err
	}

	return objStorage.Put(ctx, objectID, blob)
}

func (d *ObjectDistributor) GetObject(ctx context.Context, objectID string) ([]byte, error) {
	objStorage, err := d.getObjectStorage(objectID)
	if err != nil {
		return nil, err
	}

	return objStorage.Get(ctx, objectID)
}

func (d *ObjectDistributor) getObjectStorage(objectID string) (ObjectStorage, error) {
	d.l.RLock()
	defer d.l.RUnlock()

	storageID := d.storageSelector.LocateStorage(objectID)
	objStorage, ok := d.storages[storageID]
	if !ok {
		return nil, fmt.Errorf("selected '%s' storage does not exist", storageID)
	}

	return objStorage, nil
}
