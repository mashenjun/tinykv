package server

import (
	"bytes"
	"context"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawGetResponse)
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	b, err := sReader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	if len(b) == 0 {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = b
	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	op := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	batch := []storage.Modify{
		{
			Data: op,
		},
	}
	resp := new(kvrpcpb.RawPutResponse)

	if err := server.storage.Write(req.GetContext(), batch); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	op := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	batch := []storage.Modify{
		{
			Data: op,
		},
	}
	resp := new(kvrpcpb.RawDeleteResponse)
	if err := server.storage.Write(req.GetContext(), batch); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawScanResponse)
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	iter := sReader.IterCF(req.GetCf())
	defer iter.Close()

	iter.Seek(req.GetStartKey())
	var iterErr error
	limit := req.GetLimit()
	for iter.Valid() && limit > 0 {
		kvPair := kvrpcpb.KvPair{}
		item := iter.Item()
		kvPair.Key = item.KeyCopy(kvPair.Key)
		kvPair.Value, iterErr = item.ValueCopy(kvPair.Value)
		if iterErr != nil {
			return nil, iterErr
		}
		resp.Kvs = append(resp.Kvs, &kvPair)
		iter.Next()
		limit--
	}
	return resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.GetResponse)
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetVersion())

	lock, err := mvccTxn.GetLock(req.GetKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	// IsLockedFor will set LockError for resp
	if lock.IsLockedFor(req.GetKey(), mvccTxn.StartTS, resp) {
		return resp, nil
	}
	value, err := mvccTxn.GetValue(req.GetKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	if len(value) == 0 {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.PrewriteResponse)
	resp.Errors = make([]*kvrpcpb.KeyError, 0)
	// nothing to operate
	if len(req.Mutations) == 0 {
		return resp, nil
	}
	keysToLatches := [][]byte{}
	for _, v := range req.Mutations {
		keysToLatches = append(keysToLatches, v.Key)
	}

	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()

	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetStartVersion())

	server.Latches.WaitForLatches(keysToLatches)
	defer func(keys [][]byte) {
		server.Latches.ReleaseLatches(keys)
	}(keysToLatches)

	for _, v := range req.Mutations {
		if v == nil {
			continue
		}
		if lock, err := mvccTxn.GetLock(v.GetKey()); err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		} else if lock != nil {
			if lock.Ts <= mvccTxn.StartTS && bytes.Compare(req.GetPrimaryLock(), lock.Primary) == 0 {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
					Locked: lock.Info(v.GetKey()),
				})
				return resp, nil
			}
		}
		if w, ts, err := mvccTxn.MostRecentWrite(v.GetKey()); err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		} else if ts > req.GetStartVersion() && w.StartTS <= req.GetStartVersion() {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    w.StartTS,
					ConflictTs: ts,
					Key:        v.GetKey(),
					Primary:    req.GetPrimaryLock(),
				},
			})
			return resp, nil
		}

		lock := &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(v.GetOp()),
		}
		mvccTxn.PutLock(v.GetKey(), lock)
		mvccTxn.PutValue(v.GetKey(), v.Value)
	}

	if len(mvccTxn.Writes()) == 0 {
		return resp, nil
	}
	if err := server.storage.Write(req.GetContext(), mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	// nothing to operate
	if len(req.Keys) == 0 {
		return resp, nil
	}

	keys := server.removeDup(req.GetKeys())
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()

	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetStartVersion())

	server.Latches.WaitForLatches(req.GetKeys())
	defer func(keys [][]byte) {
		server.Latches.ReleaseLatches(keys)
	}(req.GetKeys())

	// check if it is rollback cases
	for _, key := range keys {
		recentW, _, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		}
		// already committed
		if recentW != nil && recentW.StartTS == mvccTxn.StartTS {
			continue
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		}
		tmpResp := &struct {
			Error *kvrpcpb.KeyError
		}{}
		if !lock.IsLockedFor(key, mvccTxn.StartTS, tmpResp) {
			continue
			// resp.Error = &kvrpcpb.KeyError{
			// 	Retryable: "retry",
			// }
			// return resp, nil
		}
		// if lock ts is earlier than txn's start time
		// this lock is not belong to current txn
		if lock.Ts < mvccTxn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "retry",
			}
			return resp, nil
		}
		write := &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    mvcc.WriteKindPut,
		}
		mvccTxn.PutWrite(key, req.GetCommitVersion(), write)
		mvccTxn.DeleteLock(key)
	}

	if len(mvccTxn.Writes()) == 0 {
		return resp, nil
	}
	if err := server.storage.Write(req.GetContext(), mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)
	resp.Pairs = make([]*kvrpcpb.KvPair, 0)
	if req.GetLimit() == 0 {
		return resp, nil
	}
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetVersion())
	sc := mvcc.NewScanner(req.GetStartKey(), mvccTxn)
	defer sc.Close()
	var count uint32 = 0
	for {
		key, value, err := sc.Next()
		if key == nil && value == nil && err == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Error: &kvrpcpb.KeyError{
				Retryable: "retry",
			},
			Key:   key,
			Value: value,
		})
		// log.Infof("key:%+v, value:%+v", key, value)
		count++
		if count >= req.GetLimit() {
			break
		}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// prepare mvccTxn
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetLockTs())

	latchKeys := [][]byte{req.GetPrimaryKey()}
	server.Latches.WaitForLatches(latchKeys)
	defer func(keys [][]byte) {
		server.Latches.ReleaseLatches(keys)
	}(latchKeys)

	// check if is committed
	w, wTs, err := mvccTxn.MostRecentWrite(req.GetPrimaryKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	// default key and write are existed in db
	if w != nil && wTs < req.GetCurrentTs() {
		if w.Kind == mvcc.WriteKindPut {
			resp.CommitVersion = wTs
		}
		return resp, nil
	}
	// check if is locked
	lock, err := mvccTxn.GetLock(req.GetPrimaryKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	if lock == nil || bytes.Compare(lock.Primary, req.GetPrimaryKey()) != 0 {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		mvccTxn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
	} else if lock.Ttl >= mvcc.PhysicalTime(req.GetCurrentTs())-mvcc.PhysicalTime(lock.Ts) {
		resp.LockTtl = lock.Ttl
		return resp, nil
	} else {
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		mvccTxn.DeleteValue(req.GetPrimaryKey())
		mvccTxn.DeleteLock(req.GetPrimaryKey())
		mvccTxn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
	}

	if len(mvccTxn.Writes()) == 0 {
		return resp, nil
	}

	if err := server.storage.Write(req.GetContext(), mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)

	if len(req.Keys) == 0 {
		return resp, nil
	}
	keys := server.removeDup(req.GetKeys())

	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	mvccTxn := mvcc.NewMvccTxn(sReader, req.GetStartVersion())

	server.Latches.WaitForLatches(req.GetKeys())
	defer func(keys [][]byte) {
		server.Latches.ReleaseLatches(keys)
	}(req.GetKeys())

	// check that all key is not committed or roll back yet
	skip := map[string]struct{}{}
	for _, key := range keys {
		w, _, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		}
		if w != nil && w.StartTS >= mvccTxn.StartTS {
			if w.Kind == mvcc.WriteKindPut {
				// already committed
				resp.Error = &kvrpcpb.KeyError{
					Abort: "abort",
				}
				return resp, nil
			} else if w.Kind == mvcc.WriteKindRollback {
				// already roll back
				skip[string(key)] = struct{}{}
			}
		}
	}

	// check that a key is locked by the current transaction
	for _, key := range keys {
		if _, ok := skip[string(key)]; ok {
			continue
		}
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, err
		}
		server.addRollback(mvccTxn, key, lock)
		// if lock == nil || lock.Ts != mvccTxn.StartTS {
		// 	mvccTxn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
		// 		StartTS: req.GetStartVersion(),
		// 		Kind:    mvcc.WriteKindRollback,
		// 	})
		// } else {
		// 	mvccTxn.DeleteLock(key)
		// 	mvccTxn.DeleteValue(key)
		// 	mvccTxn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
		// 		StartTS: req.GetStartVersion(),
		// 		Kind:    mvcc.WriteKindRollback,
		// 	})
		// }
	}

	if len(mvccTxn.Writes()) == 0 {
		return resp, nil
	}

	if err := server.storage.Write(req.GetContext(), mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)

	sReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	defer sReader.Close()
	mvccTxn := mvcc.NewMvccTxn(sReader, req.StartVersion)
	var filter func( *mvcc.MvccTxn,  []byte) (bool, error)
	if req.GetCommitVersion() == 0 {
		// rollback
		filter = server.canRollback
	} else {
		// commit
		filter = server.canCommit
	}
	lScanner := mvcc.NewLockScanner(mvccTxn)
	defer lScanner.Close()
	for {
		key, lock, err := lScanner.Next()
		if key == nil && lock == nil && err == nil {
			break
		}
		if ok, err := filter(mvccTxn, key); err != nil {
			switch e := err.(type) {
			case *mvcc.KeyError:
				resp.Error = &e.KeyError
			default:
				resp.RegionError = util.RaftstoreErrToPbError(e)
			}
			return resp, nil
		}else if !ok {
			continue
		}
		if req.GetCommitVersion() == 0 {
			// rollback
			server.addRollback(mvccTxn, key, lock)
		} else {
			// commit
			server.addCommit(mvccTxn, key, req.GetCommitVersion())
		}
	}

	if len(mvccTxn.Writes()) == 0 {
		return resp, nil
	}

	if err := server.storage.Write(req.GetContext(), mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

func (server *Server) removeDup(origin [][]byte) [][]byte {
	keys := make([][]byte, 0)
	dup := map[string]struct{}{}
	for _, key := range origin {
		if len(key) == 0 {
			continue
		}
		if _, ok := dup[string(key)]; !ok {
			dup[string(key)] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys
}

func (server *Server) addRollback(txn *mvcc.MvccTxn, key []byte, lock *mvcc.Lock) {
	if lock == nil || lock.Ts != txn.StartTS {
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	} else {
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
}

func (server *Server) addCommit(txn *mvcc.MvccTxn, key []byte, commitTs uint64) {
	write := &mvcc.Write{
		StartTS: txn.StartTS,
		Kind:    mvcc.WriteKindPut,
	}
	txn.PutWrite(key, commitTs, write)
	txn.DeleteLock(key)
}

func (server *Server) canCommit(txn *mvcc.MvccTxn, key []byte) (bool, error) {
	recentW, _, err := txn.MostRecentWrite(key)
	if err != nil {
		return false, err
	}
	// already committed
	if recentW != nil && recentW.StartTS == txn.StartTS {
		return false, nil
	}

	lock, err := txn.GetLock(key)
	if err != nil {
		return false, err
	}
	tmpResp := &struct {
		Error *kvrpcpb.KeyError
	}{}
	if !lock.IsLockedFor(key, txn.StartTS, tmpResp) {
		return false, &mvcc.KeyError{kvrpcpb.KeyError{Retryable: "retry"}}
	}
	// if lock ts is earlier than txn's start time
	// this lock is not belong to current txn
	if lock.Ts < txn.StartTS {
		return false, &mvcc.KeyError{kvrpcpb.KeyError{Retryable: "retry"}}
	}
	return true, nil
}

func (server *Server) canRollback(txn *mvcc.MvccTxn, key []byte) (bool, error) {
	w, _, err := txn.MostRecentWrite(key)
	if err != nil {
		return false, err
	}
	if w != nil && w.StartTS >= txn.StartTS {
		if w.Kind == mvcc.WriteKindPut {
			// already committed
			return false, &mvcc.KeyError{kvrpcpb.KeyError{Abort: "abort"}}
		} else if w.Kind == mvcc.WriteKindRollback {
			// already roll back
			return false, nil
		}
	}
	return true, nil
}
