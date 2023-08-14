package memory

import (
	"context"

	"github.com/spacelift-io/homework-object-storage/internal/core"
)

type ObjectStorage struct {
	database map[string][]byte
}

func NewObjectStorage() *ObjectStorage {
	return &ObjectStorage{
		database: make(map[string][]byte),
	}
}

func (o *ObjectStorage) Put(ctx context.Context, objectID string, object []byte) error {
	o.database[objectID] = object
	return nil
}

func (o *ObjectStorage) Get(ctx context.Context, objectID string) ([]byte, error) {
	object, ok := o.database[objectID]
	if !ok {
		return nil, core.ErrNotFound
	}
	return object, nil
}

func (o *ObjectStorage) ObjectCount() int {
	return len(o.database)
}
