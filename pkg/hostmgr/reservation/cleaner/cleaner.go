// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cleaner

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/pkg/hostmgr/factory/operation"
	hostmgrmesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	"github.com/uber/peloton/pkg/hostmgr/reservation"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	"github.com/uber/peloton/pkg/storage"
)

const (
	_defaultContextTimeout = 10 * time.Second
)

// Cleaner is the interface to recycle the to be deleted volumes and reserved resources.
type Cleaner interface {
	Run(isRunning *atomic.Bool)
}

// cleaner implements interface Cleaner to recycle reserved resources.
type cleaner struct {
	offerPool                  offerpool.Pool
	scope                      tally.Scope
	volumeStore                storage.PersistentVolumeStore
	mSchedulerClient           mpb.SchedulerClient
	mesosFrameworkInfoProvider hostmgrmesos.FrameworkInfoProvider
}

// NewCleaner initializes the reservation resource cleaner.
func NewCleaner(
	pool offerpool.Pool,
	scope tally.Scope,
	volumeStore storage.PersistentVolumeStore,
	schedulerClient mpb.SchedulerClient,
	frameworkInfoProvider hostmgrmesos.FrameworkInfoProvider) Cleaner {

	return &cleaner{
		offerPool:                  pool,
		scope:                      scope,
		volumeStore:                volumeStore,
		mSchedulerClient:           schedulerClient,
		mesosFrameworkInfoProvider: frameworkInfoProvider,
	}
}

// Run will clean the unused reservation resources and volumes.
func (c *cleaner) Run(isRunning *atomic.Bool) {
	log.Info("Cleaning reserved resources and volumes")
	c.cleanUnusedVolumes()
	log.Info("Cleaning reserved resources and volumes returned")
}

func (c *cleaner) cleanUnusedVolumes() {
	reservedOffers, _ := c.offerPool.GetOffers(summary.Reserved)
	for hostname, offerMap := range reservedOffers {
		for _, offer := range offerMap {
			if err := c.cleanOffer(offer); err != nil {
				log.WithError(err).WithFields(log.Fields{
					"hostname": hostname,
					"offer":    offer,
				}).Error("failed to clean offer")
				continue
			}
		}
	}
}

func (c *cleaner) cleanReservedResources(offer *mesos.Offer, reservationLabel string) error {
	log.WithFields(log.Fields{
		"offer": offer,
		"label": reservationLabel,
	}).Info("Cleaning reserved resources")

	// Remove given offer from memory before destroy/unreserve.
	c.offerPool.RemoveReservedOffer(offer.GetHostname(), offer.GetId().GetValue())

	operations := []*hostsvc.OfferOperation{
		{
			Type: hostsvc.OfferOperation_UNRESERVE,
			Unreserve: &hostsvc.OfferOperation_Unreserve{
				Label: reservationLabel,
			},
		},
	}
	return c.callMesosForOfferOperations(offer, operations)
}

func (c *cleaner) cleanVolume(offer *mesos.Offer, volumeID string, reservationLabel string) error {
	log.WithFields(log.Fields{
		"offer":     offer,
		"volume_id": volumeID,
		"label":     reservationLabel,
	}).Info("Cleaning volume resources")

	// Remove given offer from memory before destroy/unreserve.
	c.offerPool.RemoveReservedOffer(offer.GetHostname(), offer.GetId().GetValue())

	operations := []*hostsvc.OfferOperation{
		{
			Type: hostsvc.OfferOperation_DESTROY,
			Destroy: &hostsvc.OfferOperation_Destroy{
				VolumeID: volumeID,
			},
		},
		{
			Type: hostsvc.OfferOperation_UNRESERVE,
			Unreserve: &hostsvc.OfferOperation_Unreserve{
				Label: reservationLabel,
			},
		},
	}
	return c.callMesosForOfferOperations(offer, operations)
}

// cleanOffer calls mesos master to destroy/unreserve resources that needs to be cleaned.
func (c *cleaner) cleanOffer(offer *mesos.Offer) error {
	reservedResources := reservation.GetLabeledReservedResources([]*mesos.Offer{offer})

	for labels, res := range reservedResources {
		if len(res.Volumes) == 0 {
			return c.cleanReservedResources(offer, labels)
		} else if c.needCleanVolume(res.Volumes[0], offer) {
			return c.cleanVolume(offer, res.Volumes[0], labels)
		}
	}
	return nil
}

func (c *cleaner) needCleanVolume(volumeID string, offer *mesos.Offer) bool {
	ctx, cancel := context.WithTimeout(context.Background(), _defaultContextTimeout)
	defer cancel()

	volumeInfo, err := c.volumeStore.GetPersistentVolume(ctx, &peloton.VolumeID{
		Value: volumeID,
	})
	if err != nil {
		// Do not clean volume if db read error.
		log.WithError(err).WithFields(log.Fields{
			"volume_id": volumeID,
			"offer":     offer,
		}).Error("Failed to read db for given volume")
		return false
	}

	if volumeInfo.GetGoalState() == volume.VolumeState_DELETED {
		volumeInfo.State = volume.VolumeState_DELETED
		err = c.volumeStore.UpdatePersistentVolume(ctx, volumeInfo)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"volume": volumeInfo,
				"offer":  offer,
			}).Error("Failed to update db for given volume")
			return false
		}
		return true
	}

	return false
}

func (c *cleaner) callMesosForOfferOperations(
	offer *mesos.Offer,
	hostOperations []*hostsvc.OfferOperation) error {

	ctx, cancel := context.WithTimeout(context.Background(), _defaultContextTimeout)
	defer cancel()

	factory := operation.NewOfferOperationsFactory(
		hostOperations,
		offer.GetResources(),
		offer.GetHostname(),
		offer.GetAgentId(),
	)
	offerOperations, err := factory.GetOfferOperations()
	if err != nil {
		return err
	}

	callType := sched.Call_ACCEPT
	msg := &sched.Call{
		FrameworkId: c.mesosFrameworkInfoProvider.GetFrameworkID(ctx),
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds:   []*mesos.OfferID{offer.GetId()},
			Operations: offerOperations,
		},
	}

	log.WithFields(log.Fields{
		"offer": offer,
		"call":  msg,
	}).Info("cleaning offer with operations")

	msid := c.mesosFrameworkInfoProvider.GetMesosStreamID(ctx)
	return c.mSchedulerClient.Call(msid, msg)
}
