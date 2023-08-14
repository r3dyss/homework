package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/client"
	gorillaHandlers "github.com/gorilla/handlers"
	"github.com/sirupsen/logrus"
	"github.com/spacelift-io/homework-object-storage/internal/client/docker"
	"github.com/spacelift-io/homework-object-storage/internal/core/distributor"
	"github.com/spacelift-io/homework-object-storage/internal/handler"
	minioStorage "github.com/spacelift-io/homework-object-storage/internal/storage/minio"
	"github.com/spacelift-io/homework-object-storage/internal/util"
)

const minioDockerStorageName = "amazin-object-storage-node"

func run() error {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}

	dockerClient := docker.NewClient(cli)

	objectDistributor := distributor.NewObjectDistributor(util.NewConsistentHashStorageSelector())
	storageLocator := util.NewMinioStorageLocator(
		func(ctx context.Context) ([]util.Container, error) {
			return dockerClient.SearchContainers(ctx, minioDockerStorageName)
		},
		func(storageID string, storage *minioStorage.ObjectStorage) {
			logrus.WithFields(logrus.Fields{
				"storageID": storageID,
			}).Info("adding storage")
			objectDistributor.AddStorage(storageID, storage)
		},
		func(storageID string) {
			logrus.WithFields(logrus.Fields{
				"storageID": storageID,
			}).Info("removing storage")
			objectDistributor.RemoveStorage(storageID)
		},
	)

	httpServer := &http.Server{
		Addr: fmt.Sprintf(":3000"),
		Handler: gorillaHandlers.RecoveryHandler(
			gorillaHandlers.RecoveryLogger(logrus.StandardLogger()),
		)(handler.Router(objectDistributor)),
	}

	serverCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		t := time.NewTicker(time.Second)
		defer t.Stop()

		for serverCtx.Err() == nil {
			select {
			case <-serverCtx.Done():
				return
			case <-t.C:
				if err := storageLocator.Tick(serverCtx); err != nil {
					logrus.WithError(err).Error("ticking server locator")
				}
			}
		}
	}()

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			logrus.WithError(err).Error("http server")
		}
		cancel()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigChan:
	case <-serverCtx.Done():
	}
	if err := httpServer.Shutdown(context.Background()); err != nil {
		logrus.WithError(err).Error("shutting down http server")
	}
	<-serverCtx.Done()

	wg.Wait()
	return nil
}

func main() {
	if err := run(); err != nil {
		logrus.WithError(err).Error("running application")
	}
}
