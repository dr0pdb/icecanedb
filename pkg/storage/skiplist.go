package storage

import (
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

// SkipList is the probabilistic data structure used in memtable.
// It supports byte key and values along with custom comparators.
//
// It requires external synchronization generally via a mutex.
type SkipList interface {
	Get(key []byte) ([]byte, error)

	Set(key, value []byte) error

	Delete(key []byte) error
}

type skipList struct {
	head       *skipListNode
	maxLevel   int
	randSource rand.Source
	comparator Comparator
}

func (s *skipList) Get(key []byte) ([]byte, error) {
	log.WithFields(log.Fields{
		"key": key,
	}).Info("skipList: Get")

	panic("not implemented")
}

func (s *skipList) Set(key, value []byte) error {
	panic("not implemented")
}

func (s *skipList) Delete(key []byte) error {
	panic("not implemented")
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

// NewSkipList creates a new SkipList
func NewSkipList(maxLevel int, comparator Comparator) SkipList {
	if maxLevel < 1 || maxLevel > 12 {
		panic("maxLevel for the SkipList must be a positive integer <= 12")
	}

	return &skipList{
		head:       &skipListNode{next: make([]*skipListNode, maxLevel)},
		maxLevel:   maxLevel,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
		comparator: comparator,
	}
}
