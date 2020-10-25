# Storage

Storage package is the persistent key-value storage layer where both key and values are arbitrarily sized bytes. This package can be used independently as a key-value storage library.

Internally, it uses Log-Structed Merge (LSM) tree. It is heavily inspired by the [LevelDB](https://github.com/google/leveldb) project.
