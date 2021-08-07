package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)

	engine := engine_util.NewEngines(kvDB,raftDB,kvPath,raftPath)
	return &StandAloneStorage{
		engine: engine,
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewLocalReader(s.engine.Kv.NewTransaction(false)),nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var writeBatch engine_util.WriteBatch
	for _, val := range batch{
		if val.Value() == nil{
			writeBatch.DeleteCF(val.Cf(),val.Key())
		}else{
			writeBatch.SetCF(val.Cf(),val.Key(),val.Value())
		}
	}
	return s.engine.WriteKV(&writeBatch)
}
