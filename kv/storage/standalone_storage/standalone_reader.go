package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type LocalReader struct {
	txn    *badger.Txn
}

func (l LocalReader) GetCF(cf string, key []byte) ([]byte, error) {
	val ,err := engine_util.GetCFFromTxn(l.txn,cf,key)
	if err == badger.ErrKeyNotFound{
		return nil,nil
	}else if err != nil{
		return nil,err
	}
	return val,nil
}

func (l LocalReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf,l.txn)
}

func (l LocalReader) Close() {
	l.txn.Discard()
}

func NewLocalReader(txn *badger.Txn) *LocalReader{
	return &LocalReader{
		txn: txn,
	}
}


