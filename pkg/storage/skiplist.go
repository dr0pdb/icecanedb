package storage

import (
	"math/rand"
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
// It requires external synchronization generally via a mutex.
type SkipList interface {
	// Get finds an element by key.
	//
	// returns a pointer to the skip list node if the key is found
	// returns nil in case the node with key is not found.
	Get(key []byte) *skipListNode

	// Set inserts a value in the list associated with the specified key.
	//
	// Overwrites the data if the key already exists.
	// returns a pointer to the inserted/modified skip list node.
	Set(key, value []byte) *skipListNode

	// Delete deletes a value in the list associated with the specified key.
	//
	// returns a pointer to the inserted/modified skip list node.
	// returns nil if the node isn't found.
	Delete(key []byte) *skipListNode
}

type skipList struct {
	head        *skipListNode
	maxLevel    int32
	comparator  Comparator
	probability float64
}

// Get finds an element by key.
//
// returns a pointer to the skip list node if the key is found.
// returns nil in case the node with key is not found.
func (s *skipList) Get(key []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: Get")

	var next *skipListNode
	prev := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		// while the user key is bigger than next.Key()
		for next != nil && s.comparator.Compare(key, next.Key()) == 1 {
			prev = next
			next = next.next[i]
		}
	}

	// next.Key() should be <= user key
	if next != nil && s.comparator.Compare(next.Key(), key) <= 0 {
		log.WithFields(log.Fields{
			"key":       string(key),
			"nextkey":   string(next.Key()),
			"nextvalue": string(next.Value()),
		}).Info("skipList: Get; Found the node.")

		return next
	}

	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: Get; Node not found.")

	return nil
}

// Set inserts a value in the list associated with the specified key.
//
// Overwrites the data if the key already exists.
// returns a pointer to the inserted/modified skip list node.
func (s *skipList) Set(key, value []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("skipList: Set")

	var element *skipListNode
	prevs := s.getPreviousNodesForAllLevels(key)

	if element = prevs[0].next[0]; element != nil && s.comparator.Compare(element.Key(), key) <= 0 {
		log.WithFields(log.Fields{
			"key":      string(key),
			"value":    string(value),
			"oldvalue": string(element.Value()),
		}).Info("skipList: Set; Found an existing key. Overriding the existing value.")

		element.value = value
		return element
	}

	log.WithFields(log.Fields{
		"key":   string(key),
		"value": string(value),
	}).Info("skipList: Set; Inserting the key with the value.")

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

// Delete deletes a value in the list associated with the specified key.
//
// returns a pointer to the inserted/modified skip list node.
// returns nil if the node isn't found.
func (s *skipList) Delete(key []byte) *skipListNode {
	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: Delete")

	prevs := s.getPreviousNodesForAllLevels(key)

	if element := prevs[0].next[0]; element != nil && s.comparator.Compare(element.Key(), key) <= 0 {
		log.WithFields(log.Fields{
			"key": string(key),
		}).Info("skipList: Delete; Found the node with the key. Removing it.")

		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		return element
	}

	log.WithFields(log.Fields{
		"key": string(key),
	}).Info("skipList: Delete; Key not found.")

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
		for next != nil && s.comparator.Compare(key, next.Key()) == 1 {
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

type skipListNode struct {
	key   []byte
	value []byte
	next  []*skipListNode
}

func (sn *skipListNode) Key() []byte {
	return sn.key
}

func (sn *skipListNode) Value() []byte {
	return sn.value
}

type skipListIterator struct {
}

// NewSkipList creates a new SkipList
//
// Passing 0 for maxLevel leads to a default max level.
func NewSkipList(maxLevel int32, comparator Comparator) SkipList {
	if maxLevel < 1 || maxLevel > 12 {
		panic("maxLevel for the SkipList must be a positive integer <= 12")
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
