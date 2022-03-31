# znslsm
Implementation of a key-value store that uses an LSM-tree on a ZNS device.
The project exists out of two subprojects, both are available in `implementation`.
The first one `device` is a library that interfaces with SPDK to access a ZNS device. This might eventually move to its own repository.
The second one `rocksdb` is a modification of `RocksDB` that uses `device` instead of a filesystem to access content on the SSD.

# decisions and architecture
Decisions are prone to change and are for now maintained in a Google Docs: https://docs.google.com/document/d/1KZPoK5zDumH4She5flyvqn_hHsLqsiS1l5FtshBQ-s0/edit#.
This will eventually move completely to this repository, but it suffices for now. 
