### 1. 前言

### 2. project1

​	这一个作业需要实现一个badger的wrapper，需要填空的代码分别在`kv/server/server.go`和`kv/storage/standalone/standalone_storage.go`中。整个作业不难，但是对于编程能力弱鸡的我而言还是花了一些时间才把整个脉络弄清楚。

> 首先建议先看一遍在`server/server_test.go`中的测试用例了解一下情况然后再去写代码。

​	第一个部分是实现`StandAloneStorage`，这个类继承`Storage`接口，实际上就是把`badger.DB`包装一下：

```go
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
```

大部分方法实现起来不难，关键在于`Reader()`这个方法需要实现一个继承`StorageReader`的类：

```go
// Implement the StorageReader inferface
type AloneStorageReader struct {
	db  *badger.DB  // 好像把db放里面也没什么用
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
  // Discard txn when the transaction finished
	r.txn.Discard()
	return
}
```

为了实现读的原子性，需要用到`badger.Txn`，这在说明文档中提到了。然后实现`Reader()`方法：

```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewAloneStoargeReader(s.db), nil
}
```

​	第一部分至此完成，然后是第二个部分，利用刚实现好的`StandAloneStorage`实现4个gRPC API：

```go
// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	resp := new(kvrpcpb.RawGetResponse)
	val, _ := reader.GetCF(req.GetCf(), req.GetKey())
	reader.Close() // 记得关闭事务
	resp.Value = val

	if val != nil {
		resp.NotFound = false
	}
	resp.NotFound = true
	return resp, nil // 这里有些疑惑，无论是否get成功err都应该是nil，这是测试用例的要求，为什么呢？ 
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawPutResponse)
	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawDeleteResponse)
	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	resp := new(kvrpcpb.RawScanResponse)
	for i := 0; uint32(i) < req.GetLimit() && iter.Valid(); i++ {
		item := iter.Item()
		val, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Error:                nil,
			Key:                  item.Key(),
			Value:                val,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		iter.Next()
	}
	iter.Close()
	reader.Close()
	return resp, nil
}
```

### 3. project2

