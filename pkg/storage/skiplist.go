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

// skipList is the probabilistic data structure used in memtable.
// It supports byte key and values along with custom comparators.
//
// It can be accessed concurrently.
type skipList struct {
	mutex       sync.RWMutex
	head        *skipListNode
	maxLevel    int32
	comparator  Comparator
	probability float64
}

// get finds an element by key.
//
// returns a pointer to the skip list node if the key is found.
// returns nil in case the node with key is not found.
func (s *skipList) get(key []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: get")

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var next *skipListNode
	prev := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		// while the user key is bigger than next.Key()
		for next != nil && s.comparator.Compare(key, next.getKey()) == 1 {
			prev = next
			next = next.next[i]
		}
	}

	// next.Key() should be <= user key
	if next != nil && s.comparator.Compare(next.getKey(), key) <= 0 {
		log.WithFields(log.Fields{
			"key":       string(key),
			"nextkey":   string(next.getKey()),
			"nextvalue": string(next.getValue()),
		}).Info("skipList: get; Found the node.")

		return next
	}

	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: get; Node not found.")

	return nil
}

// set inserts a value in the list associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a pointer to the inserted/modified skip list node.
func (s *skipList) set(key, value []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("skipList: set")

	s.mutex.Lock()
	defer s.mutex.Unlock()

	var element *skipListNode
	prevs := s.getPreviousNodesForAllLevels(key)

	if element = prevs[0].next[0]; element != nil && s.comparator.Compare(element.getKey(), key) <= 0 {
		log.WithFields(log.Fields{
			"key":      string(key),
			"value":    string(value),
			"oldvalue": string(element.getValue()),
		}).Info("skipList: set; Found an existing key. Overriding the existing value.")

		element.value = value
		return element
	}

	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("skipList: set; Inserting the key with the value.")

	element = &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, s.randomLevel()),
	}

	for i := range element.next {
		element.next[i] = prevs[i].next[i]
		prevs[i].next[i] = element
	}

	return element
}

// delete deletes a value in the list associated with the specified key.
//
// returns a pointer to the inserted/modified skip list node.
// returns nil if the node isn't found.
func (s *skipList) delete(key []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: delete")

	s.mutex.Lock()
	defer s.mutex.Unlock()

	prevs := s.getPreviousNodesForAllLevels(key)

	if element := prevs[0].next[0]; element != nil && s.comparator.Compare(element.getKey(), key) <= 0 {
		log.WithFields(log.Fields{
			"key": string(key),
		}).Info("skipList: delete; Found the node with the key. Removing it.")

		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		return element
	}

	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: delete; Key not found.")

	return nil
}

// front returns the first node of the skip list.
func (s *skipList) front() *skipListNode {
	return s.head.next[0]
}

// getPreviousNodesForAllLevels returns the previous nodes at each level for passed in key.
func (s *skipList) getPreviousNodesForAllLevels(key []byte) []*skipListNode {
	prevs := s.cloneHeadNext() // careful not to modify the original head pointer contents.
	var next *skipListNode
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
func (s *skipList) cloneHeadNext() []*skipListNode {
	var clone []*skipListNode = make([]*skipListNode, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		clone[i] = s.head.next[i]
	}

	return clone
}

func (s *skipList) randomLevel() int32 {
	var level int32 = 1

	for level < s.maxLevel && rand.Float64() > s.probability {
		level++
	}

	return level
}

// getEqualOrGreater returns the skiplist node with key >= the passed key.
// nil key denotes -inf i.e. the smallest.
// obtains a read lock on the skip list internally.
// return nil if no such node exists.
func (s *skipList) getEqualOrGreater(key []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: getEqualOrGreater")

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var next *skipListNode
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
		}).Info("skipList: getEqualOrGreater; no node found.")
	}

	return next
}

// newSkipListIterator returns a new skip list iterator on the skip list.
// requires external synchronization.
func (s *skipList) newSkipListIterator() *skipListIterator {
	return &skipListIterator{
		skipList: s,
		node:     nil,
	}
}

type skipListNode struct {
	key   []byte
	value []byte
	next  []*skipListNode
}

func (sn *skipListNode) getKey() []byte {
	return sn.key
}

func (sn *skipListNode) getValue() []byte {
	return sn.value
}

// skipListIterator is the iterator over the key-value pairs of the skip list.
// It relies on the internal synchronization of the skiplist.
// Multiple threads can access different iterators
// but two threads accessing the same iterator requires external synchronization.
type skipListIterator struct {
	skipList *skipList
	node     *skipListNode
}

var _ Iterator = (*skipListIterator)(nil)

// Checks if the current position of the iterator is valid.
func (sli *skipListIterator) Valid() bool {
	return sli.node != nil
}

// Move to the first entry of the skiplist.
// Call Valid() to ensure that the iterator is valid after the seek.
func (sli *skipListIterator) SeekToFirst() {
	sli.node = sli.skipList.getEqualOrGreater(nil)
}

// Seek the iterator to the first element whose key is >= target
// Call Valid() to ensure that the iterator is valid after the seek.
func (sli *skipListIterator) Seek(target []byte) {
	sli.node = sli.skipList.getEqualOrGreater(target)
}

// Moves to the next key-value pair in the skiplist.
// Call valid() to ensure that the iterator is valid.
// REQUIRES: Current position of iterator is valid. Panic otherwise.
func (sli *skipListIterator) Next() {
	if !sli.Valid() {
		panic("Next on an invalid iterator position in skiplist.")
	}
	sli.node = sli.node.next[0]
}

// Get the key of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (sli *skipListIterator) Key() []byte {
	if !sli.Valid() {
		panic("Key on an invalid iterator position in skiplist.")
	}
	return sli.node.getKey()
}

// Get the value of the current iterator position.
// REQUIRES: Current position of iterator is valid. Panics otherwise.
func (sli *skipListIterator) Value() []byte {
	if !sli.Valid() {
		panic("Value on an invalid iterator position in skiplist.")
	}
	return sli.node.getValue()
}

// newSkipList creates a new skipList
//
// Passing 0 for maxLevel leads to a default max level.
func newSkipList(maxLevel int32, comparator Comparator) *skipList {
	if maxLevel < 1 || maxLevel > 18 {
		panic("maxLevel for the SkipList must be a positive integer <= 18")
	}

	if maxLevel == 0 {
		maxLevel = defaultMaxLevel
	}

	rand.Seed(time.Now().Unix())

	return &skipList{
		head:        &skipListNode{next: make([]*skipListNode, maxLevel)},
		maxLevel:    maxLevel,
		comparator:  comparator,
		probability: defaultProbability,
	}
}
