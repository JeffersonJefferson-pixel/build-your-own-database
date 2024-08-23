package main

type Scanner struct {
	// the range from key1 to key2
	Cmp1 int
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	db      *DB
	tdef    *TableDef
	indexNo int // -1: use primary key; >= 0; use an index
	iter    *BIter
	keyEnd  []byte
}

// within the range or not?
func (sc Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

// move the underlying b-tree iterator
func (sc *Scanner) Next() {
	assert(sc.Valid(), ErrSomethingWentWrong)
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// fetch the current row()
func (sc *Scanner) Deref(rec *Record) {
	assert(sc.Valid(), ErrSomethingWentWrong)

	tdef := sc.tdef
	rec.Cols = tdef.Cols
	rec.Vals = rec.Vals[:0]
	key, val := sc.iter.Deref()

	if sc.indexNo < 0 {
		// primary key, decode the KV piar
	} else {
		// secondary key
		assert(len(val) == 0, ErrSomethingWentWrong)

		// decode the primary key first
		index := tdef.Indexes[sc.indexNo]
		ival := make([]Value, len(index))
		for i, c := range index {
			ival[i].Type = tdef.Types[colIndex(tdef, c)]
		}
		decodeValues(key[4:], ival)
		icol := Record{index, ival}

		// fetch the row by the primary key
		rec.Cols = tdef.Cols[:tdef.PKeys]
		for _, c := range rec.Cols {
			rec.Vals = append(rec.Vals, *icol.Get(c))
		}

		ok, err := dbGet(sc.db, tdef, rec)
		assert(ok && err == nil, ErrSomethingWentWrong)
	}
}
