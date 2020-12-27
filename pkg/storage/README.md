# Storage

Storage package is the persistent key-value storage layer where both key and values are arbitrarily sized bytes. This package can be used independently as a key-value storage library but isn't suitable for production use and never will be. When I started writing this, I had planned to make this a feature complete port of [LevelDB](https://github.com/google/leveldb) to Go since the [official one](https://github.com/golang/leveldb) isn't quite complete. Weeks later, I realized that I didn't have the time and resources to make this a production ready port.

As a result, this is an incomplete port of the [LevelDB](https://github.com/google/leveldb) project which kind of works for my personal toy database use case. It doesn't implement numerous features most notably compaction. Bunch of improvements can be done in numerous places. It might have been better to just use an existing key value storage library like [Badger](https://github.com/dgraph-io/badger) but I learnt a lot while doing this.

Internally, it uses Log-Structed Merge (LSM) tree and a skiplist to store key value data persistently. I've written a few [blog posts](https://sauravt.me/tags/leveldb/) on LevelDB in case you're interested.
