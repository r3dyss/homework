package minio

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testEnvironment struct {
	minioClient *minio.Client
}

func createMinioClient() (*minio.Client, func(), error) {
	minioPort, err := nat.NewPort("", "9000")
	if err != nil {
		return nil, nil, err
	}

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ACCESS_KEY": "accessKey",
			"MINIO_SECRET_KEY": "secretKey",
		},
		Cmd: []string{"server", "/data"},
		//WaitingFor: wait.ForLog("Endpoint:  http://0.0.0.0:9000"),
		WaitingFor: wait.ForListeningPort(minioPort),
	}

	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, err
	}

	mappedPort, err := container.MappedPort(context.Background(), "9000/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	hostIP, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	client, err := minio.New(fmt.Sprintf("%s:%s", hostIP, mappedPort.Port()), &minio.Options{
		Creds:  credentials.NewStaticV4("accessKey", "secretKey", ""),
		Secure: false,
	})
	if err != nil {
		container.Terminate(context.TODO())
		return nil, nil, err
	}

	return client, func() {
		container.Terminate(context.Background())
	}, nil
}

func TestMain(m *testing.M) {
	minioClient, cleanup, err := createMinioClient()
	if err != nil {
		logrus.WithError(err).Error("creating minio client")
		os.Exit(0)
	}
	testEnvironment.minioClient = minioClient

	retCode := m.Run()
	cleanup()
	os.Exit(retCode)
}
