package cosmosds

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	blocks "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	iscn "github.com/likecoin/iscn-ipld/plugin/block"

	cid "github.com/ipfs/go-cid"
	tmrpc "github.com/tendermint/tendermint/rpc/client"
)

// Datastore is the main structure.
type Datastore struct {
	*accessor
	DB   *leveldb.DB
	path string
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)

// Options is an alias of syndtr/goleveldb/opt.Options which might be extended
// in the future.
type Options opt.Options

// NewDatastore returns a new datastore backed by leveldb
//
// for path == "", an in memory bachend will be chosen
func NewDatastore(path, tmEndpoint string, opts *Options) (*Datastore, error) {
	var nopts opt.Options
	if opts != nil {
		nopts = opt.Options(*opts)
	}

	var err error
	var db *leveldb.DB

	if path == "" {
		db, err = leveldb.Open(storage.NewMemStorage(), &nopts)
	} else {
		db, err = leveldb.OpenFile(path, &nopts)
		if errors.IsCorrupted(err) && !nopts.GetReadOnly() {
			db, err = leveldb.RecoverFile(path, &nopts)
		}
	}

	if err != nil {
		return nil, err
	}

	tmClient := tmrpc.NewHTTP(tmEndpoint, "/websocket")

	ds := Datastore{
		accessor: &accessor{
			ldb:        db,
			syncWrites: true,
			closeLk:    new(sync.RWMutex),
			tmClient:   tmClient,
		},
		DB:   db,
		path: path,
	}
	return &ds, nil
}

// An extraction of the common interface between LevelDB Transactions and the DB itself.
//
// It allows to plug in either inside the `accessor`.
type levelDbOps interface {
	Put(key, value []byte, wo *opt.WriteOptions) error
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	Delete(key []byte, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Datastore operations using either the DB or a transaction as the backend.
type accessor struct {
	ldb        levelDbOps
	syncWrites bool
	closeLk    *sync.RWMutex
	tmClient   *tmrpc.HTTP
}

func isISCN(key ds.Key) (bool, *cid.Cid) {
	if !blocks.BlockPrefix.Equal(key.Parent()) {
		return false, nil
	}

	c, err := dshelp.DsKeyToCid(ds.NewKey(key.BaseNamespace()))
	if err != nil {
		return false, nil
	}

	return iscn.IsIscnObject(c.Type()), &c
}

func (a *accessor) tmGet(path string, cid cid.Cid) ([]byte, error) {
	result, err := a.tmClient.ABCIQuery(path, cid.Bytes())
	if err != nil {
		return nil, err
	}
	code := result.Response.GetCode()
	if result.Response.GetCode() != 0 {
		return nil, fmt.Errorf("tendermint query response with code %d: %s", code, err.Error())
	}
	return result.Response.GetValue(), nil
}

func (a *accessor) Put(key ds.Key, value []byte) (err error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Put(key.Bytes(), value, &opt.WriteOptions{Sync: a.syncWrites})
}

func (a *accessor) Sync(prefix ds.Key) error {
	return nil
}

func (a *accessor) Get(key ds.Key) (value []byte, err error) {
	if ok, cid := isISCN(key); ok {
		return a.tmGet("/custom/iscn/cid_get", *cid)
	}

	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	val, err := a.ldb.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

func (a *accessor) Has(key ds.Key) (exists bool, err error) {
	if ok, cid := isISCN(key); ok {
		res, err := a.tmGet("/custom/iscn/cid_has", *cid)
		if err != nil {
			return false, err
		}
		if len(res) != 1 {
			return false, fmt.Errorf("invalid response length (%d) from tendermint for cid_has", len(res))
		}
		return res[0] != 0, nil
	}

	//TODO: Should check key for Cosmos
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Has(key.Bytes(), nil)
}

func (a *accessor) GetSize(key ds.Key) (size int, err error) {
	if ok, cid := isISCN(key); ok {
		res, err := a.tmGet("/custom/iscn/cid_get_size", *cid)
		if err != nil {
			return 0, err
		}
		if len(res) != 8 {
			return 0, fmt.Errorf("invalid response length (%d) from tendermint for cid_has", len(res))
		}
		binary.BigEndian.Uint64(res)
		return 0, nil
	}

	return ds.GetBackedSize(a, key)
}

func (a *accessor) Delete(key ds.Key) (err error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Delete(key.Bytes(), &opt.WriteOptions{Sync: a.syncWrites})
}

func (a *accessor) Query(q dsq.Query) (dsq.Results, error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	var rnge *util.Range

	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	i := a.ldb.NewIterator(rnge, nil)
	next := i.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			qNaive.Orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			next = func() bool {
				next = i.Prev
				return i.Last()
			}
			qNaive.Orders = nil
		default:
		}
	}
	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			if !next() {
				return dsq.Result{}, false
			}
			k := string(i.Key())
			e := dsq.Entry{Key: k, Size: len(i.Value())}

			if !q.KeysOnly {
				buf := make([]byte, len(i.Value()))
				copy(buf, i.Value())
				e.Value = buf
			}
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			i.Release()
			return nil
		},
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

// DiskUsage returns the current disk size used by this levelDB.
// For in-mem datastores, it will return 0.
func (d *Datastore) DiskUsage() (uint64, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.path == "" { // in-mem
		return 0, nil
	}

	var du uint64

	err := filepath.Walk(d.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		du += uint64(info.Size())
		return nil
	})

	if err != nil {
		return 0, err
	}

	return du, nil
}

// Close the LevelDB
func (d *Datastore) Close() (err error) {
	d.closeLk.Lock()
	defer d.closeLk.Unlock()
	return d.DB.Close()
}

type leveldbBatch struct {
	b          *leveldb.Batch
	db         *leveldb.DB
	closeLk    *sync.RWMutex
	syncWrites bool
}

// Batch creates the LevelDB batch operator
func (d *Datastore) Batch() (ds.Batch, error) {
	return &leveldbBatch{
		b:          new(leveldb.Batch),
		db:         d.DB,
		closeLk:    d.closeLk,
		syncWrites: d.syncWrites,
	}, nil
}

func (b *leveldbBatch) Put(key ds.Key, value []byte) error {
	b.b.Put(key.Bytes(), value)
	return nil
}

func (b *leveldbBatch) Commit() error {
	b.closeLk.RLock()
	defer b.closeLk.RUnlock()
	return b.db.Write(b.b, &opt.WriteOptions{Sync: b.syncWrites})
}

func (b *leveldbBatch) Delete(key ds.Key) error {
	b.b.Delete(key.Bytes())
	return nil
}

// A leveldb transaction embedding the accessor backed by the transaction.
type transaction struct {
	*accessor
	tx *leveldb.Transaction
}

func (t *transaction) Commit() error {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	return t.tx.Commit()
}

func (t *transaction) Discard() {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	t.tx.Discard()
}

// NewTransaction creates a transation
func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	tx, err := d.DB.OpenTransaction()
	if err != nil {
		return nil, err
	}
	accessor := &accessor{ldb: tx, syncWrites: false, closeLk: d.closeLk}
	return &transaction{accessor, tx}, nil
}
