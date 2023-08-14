package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/docker/docker/client"
	gorillaHandlers "github.com/gorilla/handlers"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/spacelift-io/homework-object-storage/internal/client/docker"
	"github.com/spacelift-io/homework-object-storage/internal/core/distributor"
	"github.com/spacelift-io/homework-object-storage/internal/handler"
	minioStorage "github.com/spacelift-io/homework-object-storage/internal/storage/minio"
)

const defaultMinioStorageBucketName = "default"

func run() error {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}

	dockerClient := docker.NewClient(cli)

	objStorages, err := getMinioStorageNodes(context.Background(), dockerClient)
	if err != nil {
		return err
	}

	objectDistributor := distributor.NewObjectDistributor(objStorages, distributor.FNVSelector)

	httpServer := &http.Server{
		Addr: fmt.Sprintf(":3000"),
		Handler: gorillaHandlers.RecoveryHandler(
			gorillaHandlers.RecoveryLogger(logrus.StandardLogger()),
		)(handler.Router(objectDistributor)),
	}

	serverCtx, cancel := context.WithCancel(context.Background())
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

	return nil
}

func main() {
	if err := run(); err != nil {
		logrus.WithError(err).Error("running application")
	}
}

func getMinioStorageNodes(ctx context.Context, dockerClient *docker.Client) (map[int]distributor.ObjectStorage, error) {
	containers, err := dockerClient.ListContainers(ctx)
	if err != nil {
		return nil, err
	}

	objStorages := make(map[int]distributor.ObjectStorage)
	for _, c := range containers {
		if !strings.Contains(c.Name, "amazin-object-storage-node") {
			continue
		}

		ipParts := strings.Split(c.IPAddress, ".")
		if len(ipParts) != 4 {
			continue
		}
		storageID, _ := strconv.Atoi(ipParts[3])

		minioClient, err := minio.New(fmt.Sprintf("%s:9000", c.IPAddress), &minio.Options{
			Creds:  credentials.NewStaticV4(c.EnvironmentVariables["MINIO_ACCESS_KEY"], c.EnvironmentVariables["MINIO_SECRET_KEY"], ""),
			Secure: false,
		})
		if err != nil {
			return nil, fmt.Errorf("creating minio storage for '%s': %w", c.IPAddress, err)
		}

		objStorage, err := minioStorage.NewObjectStorage(ctx, minioClient, defaultMinioStorageBucketName)
		if err != nil {
			return nil, fmt.Errorf("creating minio object storage: %w", err)
		}
		objStorages[storageID] = objStorage
	}

	return objStorages, nil
}
