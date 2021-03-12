package mvcc

import "testing"

/*
	Since the mvcc layer is the core of the kv service, this testing suite is the core of the tests.
	Testing of the raft layer is also done via this layer itself.
*/

func setupRaftServer() {

}

func cleanupRaftServer() {

}

func setup() {
	setupRaftServer()
}

func cleanup() {
	cleanupRaftServer()
}

func TestSingleGetSetDelete(t *testing.T) {}
