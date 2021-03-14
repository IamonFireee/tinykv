package standalone_storage

import (
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
	engines *engine_util.Engines
	conf    *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{engines: nil, conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	kv := engine_util.CreateDB(s.conf.DBPath, false)
	s.engines = engine_util.NewEngines(kv, nil, s.conf.DBPath, "")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// Do i need to close a nil DB?
	return s.engines.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var _batch engine_util.WriteBatch
	for _, modify := range batch {
		_batch.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	_batch.SetSafePoint()
	return _batch.WriteToDB(s.engines.Kv)
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	iterCount int
}

func NewStandAloneStorageReader(storage *StandAloneStorage) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		storage: storage,
		iterCount: 0,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.storage.engines.Kv, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// How to init a badger.Txn?
	txn:=badger.Txn{}

	return engine_util.NewCFIterator(cf,&txn)
}

func (r *StandAloneStorageReader) Close() {
	if r.iterCount>0{
		panic("Unclosed iterator")
	}
}
