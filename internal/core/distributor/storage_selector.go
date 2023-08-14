package distributor

import (
	"context"
	"fmt"
	"hash/fnv"
)

func FNVSelector(ctx context.Context, objectID string, storageIDs []int) (int, error) {
	if len(storageIDs) == 0 {
		return 0, fmt.Errorf("no storageIDs are given")
	}

	hash := fnv.New64a()
	_, err := hash.Write([]byte(objectID))
	if err != nil {
		return 0, err
	}
	hashedID := hash.Sum64()

	storageID := hashedID % uint64(len(storageIDs))
	return storageIDs[storageID], nil
}
