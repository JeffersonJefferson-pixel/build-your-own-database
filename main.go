package main

import (
	"fmt"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok, ErrSomethingWentWrong)
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= BTREE_PAGE_SIZE, ErrBNodeTooLarge)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data == nil, ErrSomethingWentWrong)
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok, ErrSomethingWentWrong)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func main() {
	var db KV
	db.Path = "testdb"
	if err := db.Open(); err != nil {
		panic(err)
	}
	key1 := "a"
	key2 := "b"

	// key1
	val1, _ := db.Get([]byte(key1))
	fmt.Printf("%s: %s\n", key1, val1)
	if err := db.Set([]byte(key1), []byte("1")); err != nil {
		panic(err)
	}
	val1, _ = db.Get([]byte(key1))
	fmt.Printf("%s: %s\n", key1, val1)

	// key 2
	val2, _ := db.Get([]byte(key2))
	fmt.Printf("%s: %s\n", key2, val2)
	if err := db.Set([]byte(key2), []byte("2")); err != nil {
		panic(err)
	}
	val2, _ = db.Get([]byte(key2))
	fmt.Printf("%s: %s\n", key2, val2)

	// delete
	if _, err := db.Del([]byte(key1)); err != nil {
		panic(err)
	}
	val1, _ = db.Get([]byte(key1))
	fmt.Printf("%s: %s\n", key1, val1)

	if err := db.Set([]byte(key1), []byte("3")); err != nil {
		panic(err)
	}
	val1, _ = db.Get([]byte(key1))
	fmt.Printf("%s: %s\n", key1, val1)

}
