package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	db := engine_util.CreateDB(kvPath, false)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(db, nil, conf.DBPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	reader := NewStandaloneStorageReader(s.engine.Kv.NewTransaction(false))
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb := engine_util.WriteBatch{}
			wb.SetCF(put.Cf, put.Key, put.Value)
			err := s.engine.WriteKV(&wb)
			if err != nil {
				return nil
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb := engine_util.WriteBatch{}
			wb.DeleteCF(delete.Cf, delete.Key)
			err := s.engine.WriteKV(&wb)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}
