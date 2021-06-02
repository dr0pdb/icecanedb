package icecanekv

import (
	"fmt"
	"testing"
	"time"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestRaftElectionBasic(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	th.checkSingleLeader(t)
}

// without network partitions, leader and it's term should remain same
func TestRaftElectionStability(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)

	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
		assert.Equal(t, leaderID, newLeaderID, "leaderid is not equal to newLeader id")
		assert.Equal(t, leaderTerm, newLeaderTerm, "leader term is not equal to new leader term")
	}
}

func TestRaftElectionLeaderDisconnectBasic(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	time.Sleep(2 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.NotEqual(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID still same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
}

// disconnect majiority of nodes to loose quorum and reconnect one to gain quorum
func TestRaftElectionQuorumLossRegain(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)

	toDisconnect := []uint64{1, 2, 3}
	if leaderID >= 4 {
		toDisconnect[2] = leaderID
	}

	for i := range toDisconnect {
		th.disconnectPeer(toDisconnect[i])
	}

	time.Sleep(2 * time.Second)
	th.checkNoLeader(t)

	// should gain quorum
	th.reconnectPeer(toDisconnect[0])
	time.Sleep(1 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.NotEqual(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID still same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
}

// disconnect all of nodes to loose quorum and reconnect all to get a new leader
func TestRaftElectionDisconnectAll(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	for i := uint64(1); i <= 5; i++ {
		th.disconnectPeer(i)
	}

	time.Sleep(2 * time.Second)
	th.checkNoLeader(t)

	// should gain quorum
	for i := uint64(1); i <= 5; i++ {
		th.reconnectPeer(i)
	}
	time.Sleep(1 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.NotEqual(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID still same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
}

// once the new leader is elected, it should be stable with stable network
func TestRaftElectionLeaderDisconnectStability(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, _ := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	time.Sleep(2 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		newLeaderID2, newLeaderTerm2 := th.checkSingleLeader(t)
		assert.Equal(t, newLeaderID, newLeaderID2, "newLeaderID is not equal to newLeaderID2")
		assert.Equal(t, newLeaderTerm, newLeaderTerm2, "newLeaderTerm is not equal to newLeaderTerm2")
	}
}

func TestRaftElectionOldLeaderNoInterference(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, _ := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	time.Sleep(2 * time.Second)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)

	// reconnect to the network after the new leader is elected
	th.reconnectPeer(leaderID)

	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		newLeaderID2, newLeaderTerm2 := th.checkSingleLeader(t)
		assert.Equal(t, newLeaderID, newLeaderID2, "newLeaderID is not equal to newLeaderID2")
		assert.Equal(t, newLeaderTerm, newLeaderTerm2, "newLeaderTerm is not equal to newLeaderTerm2")
	}
}

func TestRaftElectionLeaderDisconnectLessThanTimeout(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)
	th.disconnectPeer(leaderID)

	time.Sleep(90 * time.Millisecond)

	th.reconnectPeer(leaderID)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.Equal(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID should be same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
	assert.Equal(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm != leaderTerm.")
}

// disconnect a follower which will make itself a candidate, then reconnect and assert that election happened
func TestRaftElectionFollowerDisconnectNewElection(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderID, leaderTerm := th.checkSingleLeader(t)
	toDisconnect := uint64(1)
	if leaderID == 1 {
		toDisconnect = 2
	}

	th.disconnectPeer(toDisconnect)
	time.Sleep(2 * time.Second)

	th.reconnectPeer(toDisconnect)
	time.Sleep(2 * time.Second)

	// an increase in the term means that a new election took place. Leader could remain the same
	_, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
}

// keep loosing and regaining quorum and check for leader
func TestRaftElectionDisconnectMultiple(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	for times := 0; times < 5; times++ {
		leaderID, leaderTerm := th.checkSingleLeader(t)
		th.disconnectPeer(leaderID)

		toDisconnect := []uint64{1, 2, 3}
		if leaderID >= 4 {
			toDisconnect[2] = leaderID
		}

		for i := range toDisconnect {
			th.disconnectPeer(toDisconnect[i])
		}

		time.Sleep(2 * time.Second)
		th.checkNoLeader(t)

		for i := range toDisconnect {
			th.reconnectPeer(toDisconnect[i])
		}
		time.Sleep(1 * time.Second)

		newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
		assert.NotEqual(t, newLeaderID, leaderID, fmt.Sprintf("error: newLeaderID still same as previous leaderID. newLeaderID: %d, leaderID: %d", newLeaderID, leaderID))
		assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")
	}
}

func TestRaftSingleWriteLeaderSucceeds(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	// should be inserted at idx 1
	err := th.kvServers[leaderId-1].RaftServer.SetValue(test.TestKeys[0], test.TestValues[0], false)
	assert.Nil(t, err, "Unexpected error while writing to leader")

	// check on each node
	for i := 0; i < 5; i++ {
		rl2 := th.kvServers[i].RaftServer.GetLogAtIndex(1)
		assert.Equal(t, test.TestKeys[0], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[0], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
	}
}

// multiple writes should succeed on a single leader
func TestRaftMultipleWriteLeaderSucceeds(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	for entry := 0; entry < 5; entry++ {
		err := th.kvServers[leaderId-1].RaftServer.SetValue(test.TestKeys[entry], test.TestValues[entry], false)
		assert.Nil(t, err, "Unexpected error while writing to leader")
	}

	// check on each node
	for node := 0; node < 5; node++ {
		for idx := uint64(1); idx <= 5; idx++ {
			rl2 := th.kvServers[node].RaftServer.GetLogAtIndex(idx)
			assert.Equal(t, test.TestKeys[idx-1], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", node+1))
			assert.Equal(t, test.TestValues[idx-1], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", node+1))
		}
	}
}

// the set request on a non-leader should be routed to a leader and successfully committed and applied
func TestRaftSingleWriteNonLeaderSucceeds(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)
	nonLeaderId := 1
	if nonLeaderId == int(leaderId) {
		nonLeaderId = 2
	}

	// should be inserted at idx 1
	err := th.kvServers[nonLeaderId-1].RaftServer.SetValue(test.TestKeys[0], test.TestValues[0], false)
	assert.Nil(t, err, "Unexpected error while writing to non leader")

	// check on each node
	for i := 0; i < 5; i++ {
		rl2 := th.kvServers[i].RaftServer.GetLogAtIndex(1)
		assert.Equal(t, test.TestKeys[0], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[0], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
	}
}

// multiple writes should succeed on different non leaders
func TestRaftMultipleWriteNonLeaderSucceeds(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, _ := th.checkSingleLeader(t)

	id := uint64(1)
	for entry := 0; entry < 5; entry++ {
		if id == leaderId {
			id++
		}

		err := th.kvServers[id-1].RaftServer.SetValue(test.TestKeys[entry], test.TestValues[entry], false)
		assert.Nil(t, err, "Unexpected error while writing to non leader")

		id++
		if id > 5 {
			id = 1
		}
	}

	// check on each node
	for node := 0; node < 5; node++ {
		for idx := uint64(1); idx <= 5; idx++ {
			rl2 := th.kvServers[node].RaftServer.GetLogAtIndex(idx)
			assert.Equal(t, test.TestKeys[idx-1], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", node+1))
			assert.Equal(t, test.TestValues[idx-1], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", node+1))
		}
	}
}

// Write key to leader, disconnect it from the network
// write another kv pair to the new leader
// check if all the nodes (except old leader) has both the kv pairs at the correct index in the raft log
func TestRaftLeaderWriteSucceedsDisconnect(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, leaderTerm := th.checkSingleLeader(t)

	// should be inserted at idx 1
	err := th.kvServers[leaderId-1].RaftServer.SetValue(test.TestKeys[0], test.TestValues[0], false)
	assert.Nil(t, err, "Unexpected error while writing to leader")

	// changes should be visible on the leader instantly
	rl := th.kvServers[leaderId-1].RaftServer.GetLogAtIndex(1)
	assert.Equal(t, test.TestKeys[0], rl.Key, "leader: saved key and returned key from log is not same")
	assert.Equal(t, test.TestValues[0], rl.Value, "leader: saved key and returned key from log is not same")

	// check on each node
	for i := 0; i < 5; i++ {
		rl2 := th.kvServers[i].RaftServer.GetLogAtIndex(1)
		assert.Equal(t, test.TestKeys[0], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[0], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
	}

	th.disconnectPeer(leaderId)

	// sleep while another leader is elected
	time.Sleep(500 * time.Millisecond)

	newLeaderID, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm")

	// should be inserted at idx 2
	err = th.kvServers[newLeaderID-1].RaftServer.SetValue(test.TestKeys[1], test.TestValues[1], false)
	assert.Nil(t, err, "Unexpected error while writing to new leader")

	// changes should be visible on the leader instantly
	rl = th.kvServers[newLeaderID-1].RaftServer.GetLogAtIndex(2)
	assert.Equal(t, test.TestKeys[1], rl.Key, "leader: saved key and returned key from log is not same")
	assert.Equal(t, test.TestValues[1], rl.Value, "leader: saved key and returned key from log is not same")

	// check on each node
	for i := 0; i < 5; i++ {
		if i == int(leaderId-1) {
			continue
		}

		rl2 := th.kvServers[i].RaftServer.GetLogAtIndex(1)
		assert.Equal(t, test.TestKeys[0], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[0], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))

		rl2 = th.kvServers[i].RaftServer.GetLogAtIndex(2)
		assert.Equal(t, test.TestKeys[1], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
		assert.Equal(t, test.TestValues[1], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", i+1))
	}
}

// disconnect a single follower and then write a few keys to the remaining cluster
// reconnect the follower and those writes should be replicated to the follower
func TestRaftFollowerCatchesUp(t *testing.T) {
	th := newIcecaneKVTestHarness()
	defer th.teardown()

	leaderId, leaderTerm := th.checkSingleLeader(t)
	toDisconnect := uint64(1)
	if leaderId == toDisconnect {
		toDisconnect = 2
	}

	th.disconnectPeer(toDisconnect)
	time.Sleep(1 * time.Second) // give disconnected node time to trigger its election

	for entry := 0; entry < 5; entry++ {
		err := th.kvServers[leaderId-1].RaftServer.SetValue(test.TestKeys[entry], test.TestValues[entry], false)
		assert.Nil(t, err, "Unexpected error while writing to leader")
	}

	th.reconnectPeer(toDisconnect)
	time.Sleep(2 * time.Second)

	_, newLeaderTerm := th.checkSingleLeader(t)
	assert.Greater(t, newLeaderTerm, leaderTerm, "error: newLeaderTerm <= leaderTerm.")

	// the reconnected node should now have all the 5 entries
	for idx := uint64(1); idx <= 5; idx++ {
		rl2 := th.kvServers[toDisconnect].RaftServer.GetLogAtIndex(idx)
		assert.Equal(t, test.TestKeys[idx-1], rl2.Key, fmt.Sprintf("On id: %d saved key and returned key from log is not same", toDisconnect+1))
		assert.Equal(t, test.TestValues[idx-1], rl2.Value, fmt.Sprintf("On id: %d saved key and returned key from log is not same", toDisconnect+1))
	}
}
