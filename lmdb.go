package lmdb

import (
	"os"

	"github.com/armon/gomdb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/localstore/engine"
)

type db struct {
	env *mdb.Env
	dbi mdb.DBI
}

func (d *db) Get(key []byte) ([]byte, error) {
	tx, err := d.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	v, err := tx.Get(d.dbi, key)
	if err == mdb.NotFound {
		return nil, nil
	}
	return v, err
}

func (d *db) GetSnapshot() (engine.Snapshot, error) {
	tx, err := d.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return nil, err
	}

	return &snapshot{d.dbi, tx}, nil
}

func (d *db) NewBatch() engine.Batch {
	return &batch{}
}

func (d *db) Commit(b engine.Batch) error {
	bt, ok := b.(*batch)
	if !ok {
		return errors.Errorf("invalid batch type %T", b)
	}

	tx, err := d.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	defer func() {
		if tx != nil {
			tx.Abort()
		}
	}()

	for _, w := range bt.Writes {
		if w.Value == nil {
			err = tx.Del(d.dbi, w.Key, nil)
		} else {
			err = tx.Put(d.dbi, w.Key, w.Value, 0)
		}

		if err != nil && err != mdb.NotFound {
			return err
		}
	}

	err = tx.Commit()
	tx = nil

	return err
}

func (d *db) Close() error {
	d.env.DBIClose(d.dbi)
	return d.env.Close()
}

type snapshot struct {
	dbi mdb.DBI
	tx  *mdb.Txn
}

func (s *snapshot) Get(key []byte) ([]byte, error) {
	v, err := s.tx.Get(s.dbi, key)
	if err == mdb.NotFound {
		return nil, nil
	}
	return v, err
}

func (s *snapshot) NewIterator(startKey []byte) engine.Iterator {
	c, err := s.tx.CursorOpen(s.dbi)
	if err != nil {
		return &iterator{err: err}
	}

	return &iterator{key: startKey, c: c, seekToStart: true}
}

func (s *snapshot) Release() {
	s.tx.Commit()
}

type iterator struct {
	key         []byte
	value       []byte
	seekToStart bool
	c           *mdb.Cursor
	err         error
}

func (it *iterator) Key() []byte {
	return it.key
}

func (it *iterator) Value() []byte {
	return it.value
}

func (it *iterator) Valid() bool {
	return it.err == nil
}

func (it *iterator) Next() bool {
	if !it.Valid() {
		return false
	}

	if it.seekToStart {
		if it.key != nil {
			it.key, it.value, it.err = it.c.Get(it.key, mdb.SET_RANGE)
		} else {
			it.key, it.value, it.err = it.c.Get(nil, mdb.FIRST)
		}

		it.seekToStart = false
	} else {
		it.key, it.value, it.err = it.c.Get(nil, mdb.NEXT)
	}

	return it.Valid()
}

func (it *iterator) Release() {
	err := it.c.Close()
	if err != nil {
		log.Errorf("close iterator error %v", err)
	}
}

type write struct {
	Key   []byte
	Value []byte
}

type batch struct {
	Writes []write
}

func (b *batch) Put(key []byte, value []byte) {
	w := write{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	b.Writes = append(b.Writes, w)
}

func (b *batch) Delete(key []byte) {
	w := write{
		Key:   append([]byte(nil), key...),
		Value: nil,
	}
	b.Writes = append(b.Writes, w)
}

type driver struct {
}

func (d driver) Open(path string) (engine.DB, error) {
	os.MkdirAll(path, 0755)

	env, err := mdb.NewEnv()
	if err != nil {
		return nil, err
	}

	if err = env.SetMaxDBs(1); err != nil {
		return nil, err
	}

	size := uint64(500 * 1024 * 1024)
	if err = env.SetMapSize(size); err != nil {
		return nil, err
	}

	flags := mdb.CREATE | mdb.NOSYNC | mdb.NOMETASYNC | mdb.WRITEMAP | mdb.MAPASYNC

	err = env.Open(path, uint(flags), 0755)
	if err != nil {
		return nil, err
	}

	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		return nil, err
	}

	dbi, err := tx.DBIOpen("", mdb.CREATE)
	if err != nil {
		tx.Abort()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &db{
		env: env,
		dbi: dbi,
	}, nil
}

func init() {
	tidb.RegisterLocalStore("lmdb", driver{})
}
