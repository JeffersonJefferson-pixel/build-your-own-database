package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

type KV struct {
	Path string // filename
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than database size
		total  int      // mmap size, can be larger than file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
		nfree   int      // number of pages taken from free list
		nappend int      // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
	free FreeList
}

var (
	ErrPageMarkedForDelete = errors.New("page is marked for delete")
)

// create initial mmap that covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}

	mmapSize := 64 << 20
	assert(mmapSize%BTREE_PAGE_SIZE == 0, errors.New("mmap is not multiple of page size"))
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// callback for BTree and FreeList, dereference a pointer.
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		assert(page != nil, ErrPageMarkedForDelete)
		return BNode{page} // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

const DB_SIG = "BuildYourOwnDB05"

// the master page format
// |sig |btree_root |page_used |
// |16B | 8B | 8B |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty the file, the master page will be created on the first write.
		db.page.flushed = 2 // reserved for the master page
		// create free list node
		head := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		db.free.head = db.free.new(head)
		flnSetHeader(head, 1, db.free.head)
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	// free list

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("Bad master page.")
	}
	db.tree.root = root
	db.page.flushed = used
	db.free.head = 2
	return nil
}

// update the master page. it must be atomic
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// ues pwrite syscall
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// callback for BTree, allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, ErrBNodeTooLarge)
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse deallocated pages
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
		db.page.updates[ptr] = node.data
		return ptr
	}
	return db.pageAppend(node)
}

// callback for BTree, deallocate a page.
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// extend the file to at least npages
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// file size is increased exponentially
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

func (db *KV) Open() error {
	// open or create a db file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// free list
	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse
	db.page.updates = map[uint64][]byte{}

	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	// done
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil, ErrSomethingWentWrong)
	}
	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(req *DeleteReq) (bool, error) {
	deleted := db.tree.Delete(req.Key)
	return deleted, flushPages(db)
}

// persist newly allocated pages
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	npages := int(db.page.flushed) + db.page.nappend + db.page.nfree
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy data to file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).data, page)
		}
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(db.page.nappend)
	db.page.nfree = 0
	db.page.nappend = 0
	clear(db.page.updates)

	// update & flush the master page
	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, ErrBNodeTooLarge)
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

func (db *KV) Update(req *InsertReq) (bool, error)
