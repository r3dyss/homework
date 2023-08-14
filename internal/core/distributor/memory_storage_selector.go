package distributor

import "hash/fnv"

type memoryStorageSelector struct {
	storages []string
	it       int
}

func newMemoryStorageSelector() *memoryStorageSelector {
	return &memoryStorageSelector{
		storages: make([]string, 0),
		it:       0,
	}
}

func (m *memoryStorageSelector) AddStorage(storageID string) {
	m.storages = append(m.storages, storageID)
}

func (m *memoryStorageSelector) RemoveStorage(storageID string) {
	for i, ID := range m.storages {
		if ID == storageID {
			m.storages = append(m.storages[:i], m.storages[i+1:]...)
			break
		}
	}
}

func (m *memoryStorageSelector) LocateStorage(objectID string) string {
	hashedID := objectIDHashed(objectID)
	return m.storages[hashedID%uint64(len(m.storages))]
}

func objectIDHashed(objectID string) uint64 {
	hash := fnv.New64a()
	_, err := hash.Write([]byte(objectID))
	if err != nil {
		return 0
	}
	return hash.Sum64()
}
