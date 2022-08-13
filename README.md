# TropoDB
TropoDB is a key-value store built directly on raw SSD storage. TropoDB was made to investigate the potential of having full control over SSD storage and what benefits such a design could bring. To get full control it makes use of (and only allows):
* NVMe ZNS SSDs, reducing the need of a Flash Translation Layer (FTL).
* [SZD](https://github.com/Krien/SimpleZNSDevice): a simple API built on top of SPDKs ZNS functionalities, removing the kernel mostly from the storage path. In other words, the database lives entirely in user-space!
* No file system at all. To get full control, the store is built directly on storage.

## Implementation
TropoDB is not an entirely new key-value store. It continues on the key-value stores [LevelDB](https://github.com/google/leveldb) and [RocksDB](https://github.com/facebook/rocksdb). In particular the API, benchmark tooling (db_bench) and memtable implementation are reused from RocksDB. Most of the key-value store logic originated from LevelDB, with some slight modifications to allow for an approach without a file system abstraction, but a SZD abstraction instead.

## ZNS key-value store design
TropoDB uses an LSM-tree design similar to many other key-value stores as it naturally fits SSDs. However, the way the LSM-tree is persisted is unique. Unique to TropoDB is its approach to design the individual LSM-tree components and divide the available storage. TropoDB claims an entire part of the SSD, runs the database in user-space and does not allow for multitenancy. Claiming an entire region, allows it to designate areas for each LSM-tree component.  Rather than building components on generic files and hoping the file system does proper hot and cold separation, each component lives in a designated region and has a specialistic storage design. We have a specialistic:
* WAL design
* L0 design
* L1..LN design
* Metadata design
For more information on how these components work, we refer to ....

## Project layout
TropoDB is a master thesis project and under heavy development. Breaking changes can and will happen. No guarantees can be made on stability.
The project exists out of two subprojects, both are available in `implementation`. `db` was an alteration of the LevelDB API, which was dropped. `rocksdb` is an alteration of RocksDB. This alteration only supports CMake as the original Makefile is disabled/removed. Changes are maintained in:
* `db_impl_switcher/*`: allows switching between conventional RocksDB and TropoDB.
* `db/zns_impl/*`: all of the logic of TropoDB
* `znsdevice/*`: Legacy SZD implementation
* `znstests/*`: test applications, tests (need updates...), benchmarking scripts

## License
The licenses are taken as they are from RocksDB. Those are quote:
"RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory). You may select, at your option, one of the above-listed licenses." 
