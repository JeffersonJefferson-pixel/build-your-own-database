package main

import (
	"encoding/binary"
	"errors"
)

/*
| type | size | total | next | pointers |
| 2B | 2B | 8B | 8B | size * 8B |
*/

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

var (
	ErrEmptyFreeListNode = errors.New("empty free list node")
)

type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

func flnSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node.data[2:4]))
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.data[12:20])
}

func flnPtr(node BNode, idx int) uint64 {
	assert(idx < flnSize(node), ErrIndexOutOfBound)
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	assert(idx < flnSize(node), ErrIndexOutOfBound)
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[2:4], size)
	binary.LittleEndian.PutUint64(node.data[12:20], next)
}

// number of items in the list
func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}
	head := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(head.data[4:12]))
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[4:12], total)
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}

		// construct a new node
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse a pointer from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// append a page to house the new node
			fl.head = fl.new(new)
		}
	}
	assert(len(reuse) == 0, ErrSomethingWentWrong)
}

// get the nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	assert(0 <= topn && topn < fl.Total(), ErrIndexOutOfBound)
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		assert(next != 0, ErrEmptyFreeListNode)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// remove `popn` pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	assert(popn <= fl.Total(), ErrSomethingWentWrong)
	if popn == 0 && len(freed) == 0 {
		return
	}

	// prepare to construct the new list
	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head)
		if popn >= flnSize(node) {
			// phase 1
			// remove all pointers in this node
			popn -= flnSize(node)
		} else {
			// phase 2
			// remove some pointers
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next node
		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0, ErrSomethingWentWrong)

	// phase 3
	// prepend new nodes
	flPush(fl, freed, reuse)

	// done
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}
