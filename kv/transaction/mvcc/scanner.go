package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey    []byte
	txn         *MvccTxn
	iterWrite   engine_util.DBIterator
	iterDefault engine_util.DBIterator
	deleted     map[string]struct{}
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	// startKey is the user key
	// check Default family
	iterWrite := txn.Reader.IterCF(engine_util.CfWrite)
	iterWrite.Seek(EncodeKey(startKey, txn.StartTS))

	sc := Scanner{
		startKey:    startKey,
		txn:         txn,
		iterWrite:   iterWrite,
		iterDefault: txn.Reader.IterCF(engine_util.CfDefault),
		deleted:     map[string]struct{}{},
	}
	return &sc
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iterWrite.Close()
	scan.iterDefault.Close()
	scan.txn.Reader.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iterWrite.Valid() {
		return nil, nil, nil
	}
	defer scan.iterWrite.Next()
	var userKey []byte
	var write *Write
	for ; scan.iterWrite.Valid(); scan.iterWrite.Next() {
		item := scan.iterWrite.Item()
		key := item.KeyCopy(nil)
		if decodeTimestamp(key) > scan.txn.StartTS {
			continue
		}
		if value, err := scan.iterWrite.Item().ValueCopy(nil); err != nil {
			return nil, nil, err
		} else {
			w, err := ParseWrite(value)
			if err != nil {
				return nil, nil, err
			}
			userKey = DecodeUserKey(key)
			if w.Kind != WriteKindPut {
				if w.Kind == WriteKindDelete {
					scan.deleted[string(userKey)] = struct{}{}
				}
				continue
			}
			if _, ok := scan.deleted[string(userKey)]; ok {
				continue
			}
			write = w
			break
		}
	}
	var dValue []byte
	for scan.iterDefault.Seek(EncodeKey(userKey, write.StartTS)); scan.iterDefault.Valid(); scan.iterDefault.Next() {
		defaultItem := scan.iterDefault.Item()
		value, err := defaultItem.ValueCopy(nil)
		if err != nil {
			return nil, nil, err
		}
		dValue = value
		break
	}
	return userKey, dValue, nil
}

type LockScanner struct {
	txn           *MvccTxn
	iterLock      engine_util.DBIterator
}

func NewLockScanner(txn *MvccTxn,) *LockScanner {
	// Your Code Here (4C).
	// startKey is the user key
	// check CfLock family
	sc := LockScanner{
		txn:           txn,
		iterLock:      txn.Reader.IterCF(engine_util.CfLock),
	}
	return &sc
}

func (scan *LockScanner) Close() {
	// Your Code Here (4C).
	scan.iterLock.Close()
	scan.txn.Reader.Close()
}

// Next of LockScanner return all lock items whose ts equals to StartTs
func (scan *LockScanner) Next() ([]byte, *Lock, error) {
	// Your Code Here (4C).
	if !scan.iterLock.Valid() {
		return nil, nil, nil
	}
	defer scan.iterLock.Next()

	for ;scan.iterLock.Valid(); scan.iterLock.Next(){
		item := scan.iterLock.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, nil, err
		}
		lock, err := ParseLock(value)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && lock.Ts == scan.txn.StartTS {
			return key, lock, nil
		}
	}
	return nil, nil, nil
}
