package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// wrapper of KV.
type DB struct {
	Path string
	// internals
	kv     KV
	tables map[string]*TableDef // cached table definition
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *DB) Scan(table string, sc *Scanner) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return fmt.Errorf("table not foudn: %s", table)
	}

	return dbScan(db, tdef, sc)
}

func (db *DB) TableNew(tdef *TableDef) error {
	// check table definition
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	assert(err == nil, ErrSomethingWentWrong)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	assert(tdef.Prefix == 0, ErrSomethingWentWrong)
	tdef.Prefix = TABLE_PREFIX_MIN
	for i := range tdef.Indexes {
		prefix := tdef.Prefix + 1 + uint32(i)
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, prefix)
	}
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	assert(err == nil, ErrSomethingWentWrong)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN, ErrSomethingWentWrong)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	ntree := 1 + uint32(len(tdef.IndexPrefixes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+ntree)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	assert(err == nil, ErrSomethingWentWrong)
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}
