// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	if !s.IsScheduleAllowed(cluster) {
		return nil
	}
	stores := cluster.GetStores()
	if len(stores) <= 1 {
		return nil
	}
	filters := []filter.Filter{
		filter.StoreStateFilter{
			ActionScope:    s.GetName(),
			MoveRegion:     true,
		},
	}
	sources := filter.SelectSourceStores(stores, filters, cluster)
	targets := filter.SelectTargetStores(stores, filters, cluster)
	if len(sources) <= 0 || len(targets) <= 0 {
		return nil
	}else if len(sources) == 1 && len(targets) == 1 && sources[0].GetID() == targets[0].GetID() {
		return nil
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})
	sourceStoreID := sources[0].GetID()
	sourceStore := sources[0]

	var sourceRegion *core.RegionInfo
	cluster.GetPendingRegionsWithLock(sourceStoreID, func(container core.RegionsContainer) {
		candidate := container.RandomRegion([]byte{}, []byte{})
		if candidate != nil && len(candidate.GetPeers())>= cluster.GetMaxReplicas() {
			sourceRegion = candidate
		}
	})
	if sourceRegion == nil {
		cluster.GetFollowersWithLock(sourceStoreID, func(container core.RegionsContainer) {
			candidate := container.RandomRegion([]byte{}, []byte{})
			if candidate != nil && len(candidate.GetPeers())>= cluster.GetMaxReplicas() {
				sourceRegion = candidate
			}
		})
	}
	if sourceRegion == nil {
		cluster.GetLeadersWithLock(sourceStoreID, func(container core.RegionsContainer) {
			candidate := container.RandomRegion([]byte{}, []byte{})
			if candidate != nil && len(candidate.GetPeers())>= cluster.GetMaxReplicas() {
				sourceRegion = candidate
			}
		})
	}
	if sourceRegion == nil {
		return nil
	}

	for i := 0; i<len(targets); i++{
		targetStoreID := targets[i].GetID()
		if targetStoreID == sourceStoreID {
			continue
		}
		distributeOnStores := sourceRegion.GetStoreIds()
		if _, ok := distributeOnStores[targetStoreID]; ok {
			continue
		}
		targetStore := cluster.GetStore(targetStoreID)
		if sourceStore.GetRegionSize() - targetStore.GetRegionSize() < sourceRegion.GetApproximateSize()<<1 {
			continue
		}

		newPeer, err:= cluster.AllocPeer(targetStoreID)
		if err != nil {
			return nil
		}
		op, err := operator.CreateMovePeerOperator("move", cluster, sourceRegion, operator.OpBalance, sourceStoreID, targetStoreID, newPeer.GetId())
		if err != nil {
			return nil
		}
		return op
	}
	return nil
}
