package storage

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// defaultMaxLevel is the default max level of the skip list
	defaultMaxLevel int32 = 12

	defaultProbability float64 = 0.5
)

// SkipList is the probabilistic data structure used in memtable.
// It supports byte key and values along with custom comparators.
//
// It can be accessed concurrently.
type SkipList struct {
	mutex       sync.RWMutex
	head        *SkipListNode
	maxLevel    int32
	comparator  Comparator
	probability float64
}

// Get finds an element by key.
//
// returns a pointer to the skip list node if the key is found.
// returns nil in case the node with key is not found.
func (s *SkipList) Get(key []byte) *SkipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("SkipList: get")

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var next *SkipListNode
	prev := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		// while the user key is bigger than next.Key()
		for next != nil && s.comparator.Compare(key, next.getKey()) == 1 {
			prev = next
			next = next.next[i]
		}
	}

	log.WithFields(log.Fields{
		"key":  string(key),
		"next": next,
	}).Debug("SkipList: get; done. returning next node")

	return next
}

// Set inserts a value in the list associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a pointer to the inserted/modified skip list node.
func (s *SkipList) Set(key, value []byte) *SkipListNode {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("SkipList: Set")

	s.mutex.Lock()
	defer s.mutex.Unlock()

	var element *SkipListNode
	prevs := s.getPreviousNodesForAllLevels(key)

	if element = prevs[0].next[0]; element != nil && s.comparator.Compare(element.getKey(), key) <= 0 {
		log.WithFields(log.Fields{
			"key":      string(key),
			"value":    string(value),
			"oldvalue": string(element.getValue()),
		}).Debug("SkipList: Set; Found an existing key. Overriding the existing value.")

		element.value = value
		return element
	}

	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Debug("SkipList: Set; Inserting the key with the value.")

	element = &SkipListNode{
		key:   key,
		value: value,
		next:  make([]*SkipListNode, s.randomLevel()),
	}

	for i := range element.next {
		element.next[i] = prevs[i].next[i]
		prevs[i].next[i] = element
	}

	return element
}

// Front returns the first node of the skip list.
func (s *SkipList) Front() *SkipListNode {
	return s.head.next[0]
}

// getPreviousNodesForAllLevels returns the previous nodes at each level for passed in key.
func (s *SkipList) getPreviousNodesForAllLevels(key []byte) []*SkipListNode {
	prevs := s.cloneHeadNext() // careful not to modify the original head pointer contents.
	var next *SkipListNode
	prev := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		// while the user key is bigger than next.Key()
		for next != nil && s.comparator.Compare(key, next.getKey()) == 1 {
			prev = next
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

// cloneHeadNext returns a deep copy of the next pointers of the head node.
func (s *SkipList) cloneHeadNext() []*SkipListNode {
	var clone []*SkipListNode = make([]*SkipListNode, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		clone[i] = s.head.next[i]
	}

	return clone
}

func (s *SkipList) randomLevel() int32 {
	var level int32 = 1

	for level < s.maxLevel && rand.Float64() > s.probability {
		level++
	}

	return level
}

// getEqualOrGreater returns the SkipList node with key >= the passed key.
// nil key denotes -inf i.e. the smallest.
// return nil if no such node exists.
func (s *SkipList) getEqualOrGreater(key []byte) *SkipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("SkipList: getEqualOrGreater")

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var next *SkipListNode
	prev := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		// while the user key is bigger than next.Key()
		for next != nil && s.comparator.Compare(key, next.getKey()) == 1 {
			prev = next
			next = next.next[i]
		}
	}

	if next == nil {
		log.WithFields(log.Fields{
			"key": string(key),
		}).Debug("SkipList: getEqualOrGreater; no node found.")
	}

	return next
}

// NewSkipListIterator returns a new skip list iterator on the skip list.
// requires external synchronization.
func (s *SkipList) NewSkipListIterator() *SkipListIterator {
	return &SkipListIterator{
		SkipList: s,
		node:     nil,
	}
}

// SkipListNode is a single node of the Skiplist
type SkipListNode struct {
	key   []byte
	value []byte
	next  []*SkipListNode
}

func (sn *SkipListNode) getKey() []byte {
	return sn.key
}

func (sn *SkipListNode) getValue() []byte {
	return sn.value
}

// SkipListIterator is the iterator over the key-value pairs of the skip list.
// It relies on the internal synchronization of the SkipList.
// Multiple threads can access different iterators
// but two threads accessing the same iterator requires external synchronization.
type SkipListIterator struct {
	SkipList *SkipList
	node     *SkipListNode
}

var _ Iterator = (*SkipListIterator)(nil)

// Valid checks if the current position of the iterator is valid.
func (sli *SkipListIterator) Valid() bool {
	return sli.node != nil
}

// SeekToFirst moves to the first entry of the SkipList.
// Call Valid() to ensure that the iterator is valid after the seek.
func (sli *SkipListIterator) SeekToFirst() {
	sli.node = sli.SkipList.getEqualOrGreater(nil)
}

// Seek the iterator to the first element whose key is >= target
// Call Valid() to ensure that the iterator is valid after the seek.
func (sli *SkipListIterator) Seek(target []byte) {
	sli.node = sli.SkipList.getEqualOrGreater(target)
}

// Next moves to the next key-value pair in the SkipList.
// Call valid() to ensure that the iterator is valid.
// REQUIRES: Current position of iterator is valid. Panic otherwise.
func (sli *SkipListIterator) Next() {
	if !sli.Valid() {
		panic("Next on an invalid iterator position in SkipList.")
	}
	sli.node = sli.node.next[0]
}

// Key returns the key of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (sli *SkipListIterator) Key() []byte {
	if !sli.Valid() {
		panic("Key on an invalid iterator position in SkipList.")
	}
	return sli.node.getKey()
}

// Value returns the value of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (sli *SkipListIterator) Value() []byte {
	if !sli.Valid() {
		panic("Value on an invalid iterator position in SkipList.")
	}
	return sli.node.getValue()
}

// NewSkipList creates a new SkipList
//
// Passing 0 for maxLevel leads to a default max level.
func NewSkipList(maxLevel int32, comparator Comparator) *SkipList {
	if maxLevel < 1 || maxLevel > 18 {
		panic("maxLevel for the SkipList must be a positive integer <= 18")
	}

	if maxLevel == 0 {
		maxLevel = defaultMaxLevel
	}

	rand.Seed(time.Now().Unix())

	return &SkipList{
		head:        &SkipListNode{next: make([]*SkipListNode, maxLevel)},
		maxLevel:    maxLevel,
		comparator:  comparator,
		probability: defaultProbability,
	}
}
