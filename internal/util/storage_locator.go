package util

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minioStorage "github.com/spacelift-io/homework-object-storage/internal/storage/minio"
)

type MinioStorageLocator struct {
	containerSearchFn ContainerSearchFn
	onStorageAdded    func(storageID string, storage *minioStorage.ObjectStorage)
	onStorageRemoved  func(storageID string)

	storageCache map[string]*minioStorage.ObjectStorage
}

type Container struct {
	Name        string
	IP          string
	Environment map[string]string
}

type OnStorageAdded func(storageID string, storage *minioStorage.ObjectStorage)

type OnStorageRemoved func(storageID string)

type ContainerSearchFn func(ctx context.Context) ([]Container, error)

func NewMinioStorageLocator(containerSearchFn ContainerSearchFn, onAddedFn OnStorageAdded, onRemovedFn OnStorageRemoved) *MinioStorageLocator {
	return &MinioStorageLocator{
		containerSearchFn: containerSearchFn,
		storageCache:      make(map[string]*minioStorage.ObjectStorage),
		onStorageAdded:    onAddedFn,
		onStorageRemoved:  onRemovedFn,
	}
}

func (l *MinioStorageLocator) Tick(ctx context.Context) error {
	if err := l.CheckCurrentNodes(); err != nil {
		return err
	}
	return l.CheckForNewStorages(ctx)
}

func (l *MinioStorageLocator) CheckCurrentNodes() error {
	for storageID, storage := range l.storageCache {
		online, err := storage.Online()
		if err != nil {
			return err
		}

		if !online {
			delete(l.storageCache, storageID)

			if l.onStorageRemoved != nil {
				l.onStorageRemoved(storageID)
			}
		}
	}
	return nil
}

func (l *MinioStorageLocator) CheckForNewStorages(ctx context.Context) error {
	containers, err := l.containerSearchFn(ctx)
	if err != nil {
		return err
	}

	for _, c := range containers {
		if _, ok := l.storageCache[c.IP]; ok {
			continue
		}

		minioClient, err := minio.New(fmt.Sprintf("%s:9000", c.IP), &minio.Options{
			Creds:  credentials.NewStaticV4(c.Environment["MINIO_ACCESS_KEY"], c.Environment["MINIO_SECRET_KEY"], ""),
			Secure: false,
		})
		if err != nil {
			return fmt.Errorf("creating minio storage for '%s': %w", c.IP, err)
		}

		objStorage, err := minioStorage.NewObjectStorage(ctx, minioClient)
		if err != nil {
			return fmt.Errorf("creating minio object storage: %w", err)
		}

		l.storageCache[c.IP] = objStorage
		if l.onStorageAdded != nil {
			l.onStorageAdded(c.IP, objStorage)
		}
	}
	return nil
}
