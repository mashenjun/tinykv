package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// embed badger to interactive with file system
	db   *badger.DB
	conf *config.Config
}

var ErrNilStorage = errors.New("storage is nul")

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	saStorage := StandAloneStorage{
		db:   engine_util.CreateDB(conf.DBPath, true),
		conf: conf,
	}
	return &saStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// do nothing
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s == nil {
		return ErrNilStorage
	}
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)

	return &simpleReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 需要把所有的操作放在一个txn中执行
	if len(batch) == 0 {
		return nil
	}
	wBatch := engine_util.WriteBatch{}
	for _, b := range batch {
		key := b.Key()
		cf := b.Cf()
		value := b.Value()
		if len(value) == 0 {
			wBatch.DeleteCF(cf, key)
		}else {
			wBatch.SetCF(cf, key, value)
		}
	}
	return wBatch.WriteToDB(s.db)
}

type simpleReader struct {
	*badger.Txn
}

// if the key can not be found, should return nil bytes and nil error
//
func (sr *simpleReader)GetCF(cf string, key []byte) ([]byte, error) {
	b, err := engine_util.GetCFFromTxn(sr.Txn, cf, key)
	if err != nil && errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return b, err
}
func (sr *simpleReader)IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.Txn)
}
func (sr *simpleReader)Close() {
	sr.Txn.Discard()
}
