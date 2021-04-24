package icecanesql

// rpcRepository is responsible for communicating with the kv service
type rpcRepository struct {
	leaderID uint64
}

// newRpcRepository creates a new rpc repository layer
func newRpcRepository() *rpcRepository {
	return &rpcRepository{
		leaderID: 1,
	}
}
