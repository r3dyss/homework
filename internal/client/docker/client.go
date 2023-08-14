package docker

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/spacelift-io/homework-object-storage/internal/util"
)

type Client struct {
	cli *client.Client
}

func NewClient(cli *client.Client) *Client {
	return &Client{
		cli: cli,
	}
}

func (c *Client) SearchContainers(ctx context.Context, name string) ([]util.Container, error) {
	dockerContainers, err := c.cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}

	namesContain := func(name string, names []string) bool {
		for _, n := range names {
			if strings.Contains(n, name) {
				return true
			}
		}
		return false
	}

	containers := make([]util.Container, 0)
	for _, dc := range dockerContainers {
		if dc.State != "running" {
			continue
		}

		if !namesContain(name, dc.Names) {
			continue
		}

		container, err := c.getContainer(ctx, dc.ID)
		if err != nil {
			return nil, fmt.Errorf("getting '%s' container: %w", dc.ID, err)
		}
		containers = append(containers, container)
	}
	return containers, nil
}

func (c *Client) getContainer(ctx context.Context, containerID string) (util.Container, error) {
	containerJson, err := c.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return util.Container{}, err
	}

	var networkSettings *network.EndpointSettings
	for _, networkSettings = range containerJson.NetworkSettings.Networks {
		break
	}

	return util.Container{
		Name:        containerJson.Name,
		IP:          networkSettings.IPAddress,
		Environment: parseEnvVars(containerJson.Config.Env),
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
