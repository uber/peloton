package aurorabridge

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/respool"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/respool/svc"
	"go.uber.org/yarpc/yarpcerrors"
)

// Bootstrapper performs required bootstrapping to start aurorabridge in a
// cluster.
type Bootstrapper struct {
	config        BootstrapConfig
	respoolClient svc.ResourcePoolServiceYARPCClient
}

// NewBootstrapper creates a new Bootstrapper.
func NewBootstrapper(
	config BootstrapConfig,
	respoolClient svc.ResourcePoolServiceYARPCClient,
) *Bootstrapper {
	config.normalize()
	return &Bootstrapper{config, respoolClient}
}

// BootstrapRespool returns the ResourcePoolID for the configured path if it
// exists, else it creates a new respool using the configured spec.
func (b *Bootstrapper) BootstrapRespool() (*peloton.ResourcePoolID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.Timeout)
	defer cancel()

	if b.config.RespoolPath == "" {
		return nil, errors.New("no path configured")
	}

	id, err := b.lookupRespoolID(ctx, b.config.RespoolPath)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			id, err = b.createDefaultRespool(ctx)
			if err != nil {
				return nil, fmt.Errorf("create default: %s", err)
			}
		} else {
			return nil, fmt.Errorf("lookup %s id: %s", b.config.RespoolPath, err)
		}
	}
	return id, nil
}

func (b *Bootstrapper) lookupRespoolID(
	ctx context.Context,
	path string,
) (*peloton.ResourcePoolID, error) {

	req := &svc.LookupResourcePoolIDRequest{
		Path: &respool.ResourcePoolPath{Value: path},
	}
	resp, err := b.respoolClient.LookupResourcePoolID(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetRespoolId(), nil
}

func (b *Bootstrapper) createDefaultRespool(
	ctx context.Context,
) (*peloton.ResourcePoolID, error) {

	root, err := b.lookupRespoolID(ctx, "/")
	if err != nil {
		return nil, fmt.Errorf("lookup root id: %s", err)
	}
	req := &svc.CreateResourcePoolRequest{
		Spec: &respool.ResourcePoolSpec{
			Name:            strings.TrimPrefix(b.config.RespoolPath, "/"),
			OwningTeam:      b.config.DefaultRespoolSpec.OwningTeam,
			LdapGroups:      b.config.DefaultRespoolSpec.LDAPGroups,
			Description:     b.config.DefaultRespoolSpec.Description,
			Resources:       b.config.DefaultRespoolSpec.Resources,
			Parent:          root,
			Policy:          b.config.DefaultRespoolSpec.Policy,
			ControllerLimit: b.config.DefaultRespoolSpec.ControllerLimit,
			SlackLimit:      b.config.DefaultRespoolSpec.SlackLimit,
		},
	}
	resp, err := b.respoolClient.CreateResourcePool(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create resource pool: %s", err)
	}
	return resp.GetRespoolId(), nil
}
