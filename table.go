package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
)

// table defintion
type TableDef struct {
	// user defined
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the primary key
	Indexes [][]string
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
	// each index is assigned a key prefix in the KV
	IndexPrefixes []uint32
}

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// get a single row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// verify input is a complete primary key
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	// encode primary key
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	// query kv store
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	// decode value
	decodeValues(val, values[tdef.PKeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)
	return true, nil
}

// reorder a record and check for missing columns
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	// initalize values array
	values := make([]Value, len(tdef.Cols))

	for i := range n {
		found := false
		for j, recCol := range rec.Cols {
			if tdef.Cols[i] == recCol {
				values = append(values, rec.Vals[j])
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found: %s", tdef.Cols[i])
		}
	}

	return values, nil
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0)
		default:
			panic("value type is unexpected")
		}
	}
	return out
}

func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}

	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func decodeValues(in []byte, out []Value)

// encode columns for the "key" of the kv
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil, ErrSomethingWentWrong)
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil, ErrSomethingWentWrong)
	return tdef
}

// modes of updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

// only deal with a complete row.
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])

	req := InsertReq{Key: key, Val: val, Mode: mode}
	added, err := db.kv.Update(&req)
	if err != nil || !req.Updated || len(tdef.Indexes) == 0 {
		return added, err
	}

	// maintain indexes
	if req.Updated && !req.Added {
		decodeValues(req.Old, values[tdef.PKeys:]) // get the old row
		indexOp(db, tdef, Record{tdef.Cols, values}, INDEX_DEL)
	}
	if req.Updated {
		indexOp(db, tdef, rec, INDEX_ADD)
	}
	return added, nil
}

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	req := DeleteReq{Key: key}
	deleted, err := db.kv.Del(&req)
	if err != nil || !deleted || len(tdef.Indexes) == 0 {
		return deleted, err
	}

	// main indexes
	if deleted {
		decodeValues(req.Old, values[tdef.PKeys:]) // get the old row
		indexOp(db, tdef, Record{tdef.Cols, values}, INDEX_DEL)
	}
	return true, nil
}

const TABLE_PREFIX_MIN = 3

func tableDefCheck(tdef *TableDef) error {
	// verify the table definition

	// verify the indexes
	for i, index := range tdef.Indexes {
		index, err := checkIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}

func dbScan(db *DB, tdef *TableDef, sc *Scanner) error {
	// sanity checks
	switch {
	case sc.Cmp1 > 0 && sc.Cmp2 < 0:
	case sc.Cmp2 > 0 && sc.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}

	values1, err := checkRecord(tdef, sc.Key1, tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := checkRecord(tdef, sc.Key2, tdef.PKeys)
	if err != nil {
		return err
	}

	sc.tdef = tdef

	// seek to start key
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	sc.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
	sc.iter = db.kv.tree.Seek(keyStart, sc.Cmp1)
	return nil
}

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}
	for _, c := range index {
		// check the index column
		if !slices.Contains(tdef.Cols, c) {
			return nil, fmt.Errorf("index not found in tdef colums: %s", c)
		}
		icols[c] = true
	}
	// add primary key to the index
	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !icols[c] {
			index = append(index, c)
		}
	}
	assert(len(index) < len(tdef.Cols), ErrSomethingWentWrong)
	return index, nil
}

func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// updating a table with secondary indexes involves multiple keys in KV store.

// maintain indexes after a record is added or removed
func indexOp(db *DB, tdef *TableDef, rec Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Value, len(tdef.Cols))
	for i, index := range tdef.Indexes {
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}

		// update the kv store
		key = encodeKey(key[:0], tdef.IndexPrefixes[i], irec[:len(index)])
		done, err := false, error(nil)
		switch op {
		case INDEX_ADD:
			done, err = db.kv.Update(&InsertReq{Key: key})
		case INDEX_DEL:
			done, err = db.kv.Del(&DeleteReq{Key: key})
		default:
			panic(fmt.Sprintf("index operation not supported: %d", op))
		}
		assert(err == nil, ErrSomethingWentWrong)
		assert(done, ErrSomethingWentWrong)
	}
}
