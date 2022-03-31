package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	txn *badger.Txn
}

func NewStandaloneStorageReader(txn *badger.Txn) *StandaloneStorageReader {
	return &StandaloneStorageReader{
		txn: txn,
	}
}

func (reader *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, error := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if error != nil && error.Error() == "Key not found" {
		error = nil
	}
	return val, error
}

func (reader *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandaloneStorageReader) Close() {
	reader.txn.Discard()
}
