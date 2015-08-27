package lmdb

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/store/localstore/engine"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	db engine.DB
}

func (s *testSuite) SetUpSuite(c *C) {
	var (
		d   driver
		err error
	)
	s.db, err = d.Open("/tmp/test-tidb-rocksdb")
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownSuite(c *C) {
	s.db.Close()
	os.RemoveAll("/tmp/test-tidb-rocksdb")
}

func (s *testSuite) TestDB(c *C) {
	db := s.db

	b := db.NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("c"))

	err := db.Commit(b)
	c.Assert(err, IsNil)

	v, err := db.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1"))

	v, err = db.Get([]byte("c"))
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	snap, err := db.GetSnapshot()
	c.Assert(err, IsNil)

	b = db.NewBatch()
	b.Put([]byte("a"), []byte("2"))
	b.Delete([]byte("b"))
	err = db.Commit(b)
	c.Assert(err, IsNil)

	v, err = snap.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1"))

	v, err = db.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("2"))

	v, err = snap.Get([]byte("c"))
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = db.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	iter := snap.NewIterator(nil)
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("a"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("b"))
	c.Assert(iter.Next(), Equals, false)

	iter.Release()

	iter = snap.NewIterator([]byte("b"))
	c.Assert(iter.Next(), Equals, true)
	c.Assert(iter.Key(), DeepEquals, []byte("b"))
	c.Assert(iter.Next(), Equals, false)

	iter.Release()

	snap.Release()
}
