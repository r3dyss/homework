package docker

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"strings"
)

type Client struct {
	cli *client.Client
}

type Container struct {
	Name                 string
	IPAddress            string
	EnvironmentVariables map[string]string
}

func NewClient(cli *client.Client) *Client {
	return &Client{
		cli: cli,
	}
}

func (c *Client) ListContainers(ctx context.Context) ([]Container, error) {
	dockerContainers, err := c.cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}

	containers := make([]Container, 0)
	for _, dc := range dockerContainers {
		container, err := c.getContainer(ctx, dc.ID)
		if err != nil {
			return nil, fmt.Errorf("getting '%s' container: %w", dc.ID, err)
		}
		containers = append(containers, container)
	}
	return containers, nil
}

func (c *Client) getContainer(ctx context.Context, containerID string) (Container, error) {
	containerJson, err := c.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return Container{}, err
	}

	var networkSettings *network.EndpointSettings
	for _, networkSettings = range containerJson.NetworkSettings.Networks {
		break
	}

	return Container{
		Name:                 containerJson.Name,
		IPAddress:            networkSettings.IPAddress,
		EnvironmentVariables: parseEnvVars(containerJson.Config.Env),
	}, nil
}

func parseEnvVars(strs []string) map[string]string {
	kvs := make(map[string]string)
	for _, kv := range strs {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			continue
		}

		kvs[parts[0]] = parts[1]
	}
	return kvs
}
