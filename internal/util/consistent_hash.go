package util

import (
	"hash/fnv"

	"github.com/buraksezer/consistent"
)

type ConsistentHashStorageSelector struct {
	consistent *consistent.Consistent
}

func NewConsistentHashStorageSelector() *ConsistentHashStorageSelector {
	return &ConsistentHashStorageSelector{
		consistent: consistent.New(nil, consistent.Config{
			Hasher: fnvHasher{},
			// Default configuration from library example
			PartitionCount:    7,
			ReplicationFactor: 20,
			Load:              1.25,
		}),
	}
}

func (c *ConsistentHashStorageSelector) AddStorage(storageID string) {
	c.consistent.Add(memberID(storageID))
}

func (c *ConsistentHashStorageSelector) RemoveStorage(storageID string) {
	c.consistent.Remove(storageID)
}

func (c *ConsistentHashStorageSelector) LocateStorage(objectID string) string {
	storageID := c.consistent.LocateKey([]byte(objectID))
	return storageID.String()
}

type memberID string

func (id memberID) String() string {
	return string(id)
}

type fnvHasher struct{}

func (h fnvHasher) Sum64(data []byte) uint64 {
	hash := fnv.New64a()
	_, err := hash.Write(data)
	if err != nil {
		return 0
	}
	return hash.Sum64()
}
