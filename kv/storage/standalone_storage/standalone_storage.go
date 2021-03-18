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
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	_storage := new(StandAloneStorage)
	_storage.conf = conf
	_storage.db = engine_util.CreateDB(conf.DBPath, false)
	return _storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// Do i need to close a nil DB?
	return s.db.Close()
}

// TODO: fucking StorageReader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return NewAloneStoargeReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var _batch engine_util.WriteBatch
	for _, modify := range batch {
		_batch.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	_batch.SetSafePoint()
	// WriteToDB is atomic through badger.Update()
	return _batch.WriteToDB(s.db)
}

// implement the StorageReader inferface
type AloneStorageReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func NewAloneStoargeReader(db *badger.DB) AloneStorageReader {
	return AloneStorageReader{db: db, txn: db.NewTransaction(false)}
}

func (r AloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(r.txn, cf, key)
	return val, nil
}

func (r AloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r AloneStorageReader) Close() {
	r.txn.Discard()
	return
}
