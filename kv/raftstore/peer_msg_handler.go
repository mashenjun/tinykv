package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	if applyRet, err := d.peerStorage.SaveReadyState(&rd); err != nil {
		log.Errorf("SaveReadyState with rd:%+v error:%v", rd, err)
		return
	} else if applyRet.Region != nil && applyRet.PrevRegion != nil {
		if util.IsEpochStale(applyRet.PrevRegion.RegionEpoch, applyRet.Region.RegionEpoch) {
			d.ctx.storeMeta.setRegion(applyRet.Region, d.peer)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applyRet.Region})
		}
	}

	kvWB := new(engine_util.WriteBatch)

	if d.RaftGroup.Raft.IsMember(d.PeerId()) {
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	}
	d.peer.Send(d.ctx.trans, rd.Messages)

	// todo apply the committedEntries to state machine
	// the data of the log entries is the bytes format of the raft_cmdpb.RaftCmdRequest,
	// unmarshal the bytes, and process the request according to their cmd type
	// log.Warnf("[P%+v] len(proposals):%+v, len(CommittedEntries):%+v, len(Entries):%+v",
	//	d.Meta.GetId(),len(d.proposals), len(rd.CommittedEntries), len(rd.Entries))
	resps := make(map[string]*raft_cmdpb.RaftCmdResponse)
	var lastCIdx uint64 = 0
	// log.Warnf("%+v cEnts:%+v", d.Tag, rd.CommittedEntries)
	destorySelf := false
	for _, cEnt := range rd.CommittedEntries {
		switch cEnt.EntryType {
		case eraftpb.EntryType_EntryNormal:
			req := new(raft_cmdpb.RaftCmdRequest)
			if err := req.Unmarshal(cEnt.Data); err != nil {
				log.Warnf("Unmarshal err:%+v", err)
				continue
			}
			lastCIdx = cEnt.Index
			key := fmt.Sprintf("%v_%v", cEnt.Term, cEnt.Index)
			switch {
			// normal command request
			case len(req.GetRequests()) > 0 && req.GetAdminRequest() == nil:
				// must check region echo again here
				if err := util.CheckRegionEpoch(req, d.Region(), true); err != nil {
					resps[key] = ErrResp(err)
					continue
				}
				for _, r := range req.GetRequests() {
					if resp, err := d.onRaftBaseCmdRequest(r, req.GetHeader()); err != nil {
						resps[key] = ErrResp(err)
					} else {
						resps[key] = resp
					}
				}
			// admin command request
			case len(req.GetRequests()) == 0 && req.GetAdminRequest() != nil:
				switch req.GetAdminRequest().GetCmdType() {
				case raft_cmdpb.AdminCmdType_Split:
					if util.IsEpochStale(req.GetHeader().GetRegionEpoch(), d.Region().GetRegionEpoch()) {
						continue
					}
					if err := d.splitRegion(req.GetAdminRequest().GetSplit()); err != nil {
						log.Errorf("%+v splitRegion err:%+v", d.Tag, err)
					}
				case raft_cmdpb.AdminCmdType_CompactLog:
					d.ScheduleCompactLog(0, req.GetAdminRequest().GetCompactLog().GetCompactIndex())
					if req.GetAdminRequest().GetCompactLog().GetCompactIndex() > d.peerStorage.applyState.TruncatedState.Index {
						d.peerStorage.applyState.TruncatedState.Term = req.GetAdminRequest().GetCompactLog().GetCompactTerm()
						d.peerStorage.applyState.TruncatedState.Index = req.GetAdminRequest().GetCompactLog().GetCompactIndex()
					}
				}
			}
		case eraftpb.EntryType_EntryConfChange:
			req := new(eraftpb.ConfChange)
			if err := req.Unmarshal(cEnt.Data); err != nil {
				log.Errorf("Unmarshal err:%+v", err)
				continue
			}
			ctx := new(kvrpcpb.Context)
			if err := ctx.Unmarshal(req.Context); err != nil {
				log.Errorf("Unmarshal err:%+v", err)
				continue
			}
			newPeer := ctx.GetPeer()
			if util.IsEpochStale(ctx.GetRegionEpoch(), d.peer.Region().GetRegionEpoch()) {
				continue
			}
			confState := d.RaftGroup.ApplyConfChange(*req)
			region := proto.Clone(d.peerStorage.Region()).(*metapb.Region)
			region.RegionEpoch.ConfVer++
			peers := make([]*metapb.Peer, 0)
			switch req.GetChangeType() {
			case eraftpb.ConfChangeType_RemoveNode:
				for _, i := range confState.Nodes {
					p := d.peer.getPeerFromCache(i)
					if p != nil {
						peers = append(peers, p)
					}
				}
				d.peer.removePeerCache(req.NodeId)
			case eraftpb.ConfChangeType_AddNode:
				for _, i := range confState.Nodes {
					p := d.peer.getPeerFromCache(i)
					if p == nil {
						peers = append(peers, newPeer)
					} else {
						if p.GetId() == req.NodeId {
							p.StoreId = newPeer.StoreId
						}
						peers = append(peers, p)
					}
				}
				// log.Infof("%v adding node (req:%v) confState:%+v idx:%+v", d.Tag, req.NodeId, confState.Nodes, cEnt.Index)
			}
			region.Peers = peers
			d.ctx.storeMeta.setRegion(region, d.peer)
			if req.GetChangeType() == eraftpb.ConfChangeType_RemoveNode && req.GetNodeId() == d.peer.Meta.Id {
				destorySelf = true
			} else if d.RaftGroup.Raft.IsMember(d.PeerId()) {
				meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			}
		}
		d.peerStorage.applyState.AppliedIndex = cEnt.Index
		kvWB.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
	}

	if err := d.peerStorage.Engines.WriteKV(kvWB); err != nil {
		panic(err)
	}

	// todo is this the correct way to notify cb???
	delBefore := -1
	for i, pol := range d.proposals {
		if pol.index > lastCIdx {
			break
		}

		key := fmt.Sprintf("%v_%v", pol.term, pol.index)
		if resp, ok := resps[key]; ok {
			// log.Warnf("[P%+v] cb resp:%+v", d.Meta.Id, resps)
			// we need to construct a txn for snapshot
			if len(resp.Responses) > 0 && resp.Responses[0].GetCmdType() == raft_cmdpb.CmdType_Snap {
				// log.Warnf("%+v cb key:%+v resp:%+v", d.Tag, key, resp)
				pol.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			pol.cb.Done(resp)
		} else {
			NotifyStaleReq(d.RaftGroup.Raft.Term, pol.cb)
		}
		delBefore = i
	}
	// clear all StaleCommand
	if delBefore >= 0 {
		d.proposals = d.proposals[delBefore+1:]
	}
	d.RaftGroup.Advance(rd)
	if destorySelf {
		d.destroyPeer()
	}
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	// if len(req.GetRequests()) != 0 && req.GetRequests()[0].GetCmdType() == raft_cmdpb.CmdType_Snap {
	// 	log.Infof("%+v req:%+v, region:%+v, err:%+v", d.Tag, req, d.Region(), err)
	// }
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.GetAdminRequest() != nil && len(msg.Requests) == 0 {
		if resp, err := d.onRaftAdminCmdRequest(msg); err != nil {
			log.Errorf("onRaftAdminCmdRequest err:%+v", err)
			cb.Done(ErrResp(err))
			return
		} else {
			cb.Done(resp)
			return
		}
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Marshal err:%+v", err)
		cb.Done(ErrResp(err))
		return
	}
	if err := d.RaftGroup.Propose(b); err != nil {
		log.Errorf("Propose err:%+v", err)
		cb.Done(ErrResp(err))
		return
	}

	// register the cb into d.proposal for later using
	// at this point, the msg is already appended into the the local log
	// cb may be nil
	if cb != nil {
		lasIdx := d.RaftGroup.Raft.RaftLog.LastIndex()
		term := d.RaftGroup.Raft.Term
		// if msg.GetRequests() != nil && msg.GetRequests()[0].GetCmdType() == raft_cmdpb.CmdType_Snap {
		// 	log.Warnf("%+v cb injection, index:%+v, term:%+v, header:%+v", d.Tag, lasIdx, term, msg.GetHeader())
		// }
		d.proposals = append(d.proposals, &proposal{
			index: lasIdx,
			term:  term,
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s, isTBS:%+v, from %+v to %+v", regionID, msg.GetMessage().GetMsgType(), msg.GetIsTombstone(), from, to)
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.

	// log.Warnf("%s checkMessage from epoch:%+v, local epoch:%+v, fromStoreID:%+v, local region:%+v", d.Tag,
	// 	fromEpoch, d.Region().GetRegionEpoch(), fromStoreID, d.Region())

	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Debugf("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	item := meta.regionRanges.Delete(&regionItem{region: d.Region()})
	if isInitialized && item == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	// log.Warnf("[P%+v]onRaftGCLogTick appliedIdx:%+v, firstIdx:%+v", d.peer.regionId, appliedIdx, firstIdx)
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
			snap.Close()
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

// RaftStore proposes the Raft command request as a Raft log
// Raft module appends the log, and persist by PeerStorage
// Raft module commits the log
// Raft worker executes the Raft command when handing Raft ready, and returns the response by callback

func (d *peerMsgHandler) onRaftBaseCmdRequest(req *raft_cmdpb.Request, header *raft_cmdpb.RaftRequestHeader) (*raft_cmdpb.RaftCmdResponse, error) {
	resp := newCmdResp()

	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		// get cmd need to query data form the kv instance db
		defer func() {
			if bytes.Compare(req.GetGet().GetKey(), []byte("k2")) == 0 {
				log.Warnf("%+v get k2:%+v", d.Tag, resp)
			}
		}()
		cf := req.GetGet().GetCf()
		key := req.GetGet().GetKey()
		if engine_util.ExceedEndKey(key, d.Region().GetEndKey()) {
			return nil, &util.ErrKeyNotInRegion{
				Key:    key,
				Region: d.Region(),
			}
		}
		val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, cf, key)
		if err != nil {
			return nil, err
		} else {
			getResp := &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get}
			getResp.Get = &raft_cmdpb.GetResponse{
				Value: val,
			}
			resp.Responses = append(resp.Responses, getResp)
			return resp, nil
		}
	case raft_cmdpb.CmdType_Put:

		// put cmd need to write data into kv instance db
		cf := req.GetPut().GetCf()
		key := req.GetPut().GetKey()
		val := req.GetPut().GetValue()
		// should do this in applied
		if err := engine_util.PutCF(d.peerStorage.Engines.Kv, cf, key, val); err != nil {
			log.Error(err)
			return nil, err
		} else {
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put,
				Put: new(raft_cmdpb.PutResponse)})
			return resp, nil
		}
	case raft_cmdpb.CmdType_Delete:
		// delete cmd need to delete data form kv instance db
		cf := req.GetDelete().GetCf()
		key := req.GetDelete().GetKey()
		if err := engine_util.DeleteCF(d.peerStorage.Engines.Kv, cf, key); err != nil {
			log.Error(err)
			return nil, err
		} else {
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete,
				Delete: new(raft_cmdpb.DeleteResponse)})
			return resp, nil
		}
	case raft_cmdpb.CmdType_Snap:
		// todo what should we do here
		snapResp := new(raft_cmdpb.SnapResponse)
		snapResp.Region = d.Region()
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap,
			Snap: snapResp,
		})
		return resp, nil
	default:

	}
	return nil, errors.New("invalid cmd type")
}

// CompactLogRequest modifies metadata, namely updates the RaftTruncatedState which is in the RaftApplyState.
// After that, you should schedule a task to raftlog-gc worker by ScheduleCompactLog.
// Raftlog-gc worker will do the actual log deletion work asynchronously.
func (d *peerMsgHandler) onRaftAdminCmdRequest(msg *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	req := msg.GetAdminRequest()
	resp := newCmdResp()
	resp.AdminResponse = new(raft_cmdpb.AdminResponse)
	resp.AdminResponse.CmdType = req.CmdType
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
	// todo
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// todo use context to save region echo info???
		// bs, _ := d.peerStorage.Region().GetRegionEpoch().Marshal()
		ctx := new(kvrpcpb.Context)
		ctx.RegionEpoch = d.peerStorage.Region().GetRegionEpoch()
		ctx.Peer = req.GetChangePeer().GetPeer()
		bs, _ := ctx.Marshal()
		cc := eraftpb.ConfChange{
			ChangeType: req.GetChangePeer().GetChangeType(),
			NodeId:     req.GetChangePeer().GetPeer().GetId(),
			Context:    bs,
		}
		if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
			log.Errorf("ProposeConfChange err:%+v", err)
			return resp, err
		}
	// case raft_cmdpb.AdminCmdType_CompactLog:
	// 	// todo should persist to raft db now?
	// 	// 异步的清理数据
	// 	d.peerStorage.applyState.TruncatedState.Term = req.GetCompactLog().GetCompactTerm()
	// 	d.peerStorage.applyState.TruncatedState.Index = req.GetCompactLog().GetCompactIndex()
	// 	log.Warnf("%v CompactLog applystate:%+v", d.Tag, d.peerStorage.applyState)
	// 	kvWB := new(engine_util.WriteBatch)
	// 	kvWB.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
	// 	if err := d.peerStorage.Engines.WriteKV(kvWB); err != nil {
	// 		panic(err)
	// 	}
	// 	d.ScheduleCompactLog(0, req.GetCompactLog().GetCompactIndex())
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.GetTransferLeader().GetPeer().GetId())
	case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_CompactLog:
		// When a Region splits into two Regions,
		// one of the Regions will inherit the metadata before splitting and just modify its Range and RegionEpoch while the other will create relevant meta information.
		// log.Warnf("%+v receive split req:%+v, header:%+v", d.Tag, req, msg.GetHeader())
		b, err := proto.Marshal(msg)
		if err != nil {
			log.Errorf("Marshal err:%+v", err)
			return resp, err
		}
		if err := d.RaftGroup.Propose(b); err != nil {
			log.Errorf("Propose err:%+v", err)
			return resp, err
		}
		// if err := d.splitRegion(req.GetSplit()); err != nil {
		// 	return resp, err
		// }
	}
	return resp, nil
}

func (d *peerMsgHandler) splitRegion(req *raft_cmdpb.SplitRequest) error {
	// modify old region metadata
	oldEndKey := d.Region().GetEndKey()
	oldRegion := proto.Clone(d.Region()).(*metapb.Region)
	oldRegion.RegionEpoch.Version++
	oldRegion.EndKey = req.GetSplitKey()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
	d.SetRegion(oldRegion)
	d.ctx.storeMeta.regions[d.regionId] = oldRegion

	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, oldRegion, rspb.PeerState_Normal)

	// create new region
	splitRegion := new(metapb.Region)
	splitRegion.Id = req.NewRegionId
	splitRegion.StartKey = req.GetSplitKey()
	splitRegion.EndKey = oldEndKey
	splitRegion.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: oldRegion.GetRegionEpoch().GetConfVer(),
		Version: oldRegion.GetRegionEpoch().GetVersion(),
	}
	// todo problem not equal; need to check
	if len(oldRegion.Peers) != len(req.GetNewPeerIds()) {
		panic(fmt.Sprintf("%+v oldRegion:%+v, req:%+v", d.Tag, oldRegion, req))
	}
	for i, peerID := range req.GetNewPeerIds() {
		storeID := oldRegion.Peers[i].StoreId
		splitRegion.Peers = append(splitRegion.Peers, &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		})
	}
	meta.WriteRegionState(kvWB, splitRegion, rspb.PeerState_Normal)
	// The corresponding Peer of this newly-created Region should be created by createPeer() and registered to the router.regions.
	// And the region’s info should be inserted into regionRanges in ctx.StoreMeta.
	// log.Warnf("%+v AdminCmdType_Split: %+v, req:%+v", d.Tag, splitRegion, req)
	peer, err := createPeer(d.ctx.store.GetId(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, splitRegion)
	if err != nil {
		return err
	}
	d.ctx.router.register(peer)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: splitRegion})
	d.ctx.storeMeta.regions[splitRegion.GetId()] = splitRegion
	if err := d.ctx.router.send(req.GetNewRegionId(), message.Msg{Type: message.MsgTypeStart}); err != nil {
		return err
	}
	// todo save both old region and new region in to kv engine
	if err := d.peerStorage.Engines.WriteKV(kvWB); err != nil {
		panic(err)
	}
	// log.Warnf("%+v splitRegion save oldRegion:%+v splitRegion:%+v, req:%+v", d.Tag, oldRegion, splitRegion, req)
	return nil
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
