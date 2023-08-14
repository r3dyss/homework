package docker

import (
	"context"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type genericContainer struct {
	Name                 string
	IP                   string
	EnvironmentVariables map[string]string
}

func TestDockerClient(t *testing.T) {
	envVariables := map[string]string{
		"TEST": "VALUE",
	}

	genericContainer, cleanup, err := createGenericContainer(context.Background(), envVariables)
	require.NoError(t, err)
	defer cleanup()

	cli, err := client.NewClientWithOpts(client.FromEnv)
	require.NoError(t, err)

	dClient := NewClient(cli)

	containers, err := dClient.ListContainers(context.Background())
	require.NoError(t, err)

	for _, c := range containers {
		if c.Name != genericContainer.Name {
			continue
		}

		assert.Equal(t, envVariables["TEST"], c.EnvironmentVariables["TEST"])
		assert.Equal(t, genericContainer.IP, c.IPAddress)
		break
	}
}

func createGenericContainer(ctx context.Context, envVariables map[string]string) (genericContainer, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        "nginx",
		Name:         "genericContainer",
		ExposedPorts: []string{"80/tcp"},
		Env:          envVariables,
		WaitingFor:   wait.ForHTTP("/"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return genericContainer{}, nil, err
	}

	ip, err := container.ContainerIP(ctx)
	if err != nil {
		container.Terminate(ctx)
		return genericContainer{}, nil, err
	}

	containerName, err := container.Name(ctx)
	if err != nil {
		container.Terminate(ctx)
		return genericContainer{}, nil, err
	}

	return genericContainer{
			Name:                 containerName,
			IP:                   ip,
			EnvironmentVariables: envVariables,
		}, func() {
			container.Terminate(context.Background())
		}, nil
}
