package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	data := storage.Put{Cf: engine_util.CfWrite, Key: EncodeKey(key, ts), Value: write.ToBytes()}
	txn.writes = append(txn.writes, storage.Modify{
		Data: data,
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	bs, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if bs == nil {
		return nil, nil
	}
	return ParseLock(bs)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	data := storage.Put{Cf: engine_util.CfLock, Key: key, Value: lock.ToBytes()}
	txn.writes = append(txn.writes, storage.Modify{
		Data: data,
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	data := storage.Delete{Cf: engine_util.CfLock, Key: key}
	txn.writes = append(txn.writes, storage.Modify{
		Data: data,
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	findWrite := false
	var wTs uint64 = 0
	iterWrite := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterWrite.Close()
	for iterWrite.Seek(EncodeKey(key, txn.StartTS)); iterWrite.Valid(); iterWrite.Next() {
		userKey := DecodeUserKey(iterWrite.Item().Key())
		if bytes.Compare(userKey, key) != 0{
			continue
		}
		value, err := iterWrite.Item().ValueCopy(nil)
		if err != nil {
			return nil, nil
		}

		w, err := ParseWrite(value)
		if err != nil {
			return nil, nil
		}
		if w.Kind == WriteKindDelete {
			return nil, nil
		} else if w.Kind == WriteKindPut {
			wTs = decodeTimestamp(iterWrite.Item().Key())
			findWrite = true
			break
		}
	}
	if !findWrite {
		return nil, nil
	}

	iterDefault := txn.Reader.IterCF(engine_util.CfDefault)
	defer iterDefault.Close()
	for iterDefault.Seek(EncodeKey(key, wTs)); iterDefault.Valid(); iterDefault.Next() {
		userKey := DecodeUserKey(iterWrite.Item().Key())
		if bytes.Compare(userKey, key) != 0{
			continue
		}
		value, err := iterDefault.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	data := storage.Put{Cf: engine_util.CfDefault, Key: EncodeKey(key, txn.StartTS), Value: value}
	txn.writes = append(txn.writes, storage.Modify{
		Data: data,
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	data := storage.Delete{Cf: engine_util.CfDefault, Key: EncodeKey(key, txn.StartTS)}
	txn.writes = append(txn.writes, storage.Modify{
		Data: data,
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterW := txn.Reader.IterCF(engine_util.CfWrite)
	end := EncodeKey(key, txn.StartTS)
	defer iterW.Close()
	for iterW.Seek(EncodeKey(key, TsMax)); iterW.Valid(); iterW.Next() {
		if engine_util.ExceedEndKey(iterW.Item().Key(), end) {
			break
		}
		userKey := DecodeUserKey(iterW.Item().Key())
		if bytes.Compare(userKey, key) != 0 {
			continue
		}
		value, err := iterW.Item().ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}
		w, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if w == nil {
			continue
		}
		commitTs := decodeTimestamp(iterW.Item().Key())
		if w.StartTS == txn.StartTS && commitTs > txn.StartTS {
			return w, commitTs, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterW := txn.Reader.IterCF(engine_util.CfWrite)
	// end := EncodeKey(key, txn.StartTS)
	defer iterW.Close()
	for iterW.Seek(EncodeKey(key, TsMax)); iterW.Valid(); iterW.Next() {
		userKey := DecodeUserKey(iterW.Item().Key())
		if bytes.Compare(userKey, key) != 0 {
			continue
		}
		value, err := iterW.Item().ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}

		w, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if w == nil {
			continue
		}
		ts := decodeTimestamp(iterW.Item().Key())
		return w, ts, nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
