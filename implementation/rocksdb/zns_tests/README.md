# ZNS_Tests
This directory contains the tests as used in the TropoDB paper.
Following the tests as described below should lead to similar results as visible in [ZNS_SPDK_benchmarks](https://github.com/Krien/ZNS_SPDK_Benchmarks/tree/main/TropoDB/data).

# Configs
The configs directory contains header files that contain valid configurations for TropoDB.
These can be copied to `TropoDB/implementation/rocksdb/db/zns_impl/tropodb_config.h` to get the configuration working within TropoDB.
Then after recompiling the project, it should run with the settings in the config, which should allow for reproducible tests.
If at any point the orignal config needs to be returned, copy `TropoDB/implementation/rocksdb/db/zns_impl/default_config.h` to
`TropoDB/implementation/rocksdb/db/zns_impl/tropodb_config.h`.

# Benchmark.sh
This is the main benchmarking script used to benchmark TropoDB and RocksDB with various file systems.
It sets up the testing environment (command `setup`), runs the tests (command `run`) and tears down the environment (command `clean`).

## Available benchmarks
The following benchmarks are available:
* long: the test used to measure garbage collection effects in TropoDB. Runs in order `1TB of fillrandom`, `1TB of filloverwrite` and `1 hour of readwhilewriting`.
Taken from ZenFS long test and altered a bit to support smart-log and BPF_trace.
* quick: Quick I/O Tests, writes 100GB with various value sizes. First sequentially and then in random order. 
Taken from ZenFS long test and altered a bit to support smart-log and BPF_trace.
* wal: runs the append tests as described in the TropoDB paper. It fills 50GB of data multiple times, each time with a different value size.
It uses a random workload.
* wal_recover: runs the WAL recovery tests as described in the TropoDB paper. It fills a few WALs and then recovers the database multiple times.
* default : LEGACY!. small test, writes 8000 key-value pairs of size 1016 bytes sequentially.

## TropoDB
For TropoDB the following is required to run a test (must be root):
```bash
sudo LD_LIBRARY_PATH=<SPDK_DIR>/dpdk/build/lib ./benchmark.sh setup tropodb <nvme_number> # No /dev/ in front!!!
# Note the used trid in a variable, for example in $TRID
sudo LD_LIBRARY_PATH=<SPDK_DIR>/dpdk/build/lib ./benchmark.sh run <benchmark_name> tropodb $TRID $TRID
sudo LD_LIBRARY_PATH=<SPDK_DIR>/dpdk/build/lib ./benchmark.sh clean tropodb $TRID $TRID
```
## F2FS
For F2FS the following is required to run a tesst (must be root):
```bash
sudo ./benchmark.sh setup f2fs <path_to_mount> <ZNS_device> <Optane_disk> # no dev for both devices!
sudo BLOCKCNT=<path_to_bpftrace_reset_script> ./benchmark.sh run f2fs <path_to_mount> <ZNS_device>
sudo ./benchmark.sh clean f2fs <path_to_mount> <ZNS_device> # no dev!
```
## ZenFS
```bash
sudo ZENFS_DIR=<ZenFS_plugin_dir>/util ./benchmark.sh setup zenfs <ZNS_device> # No /dev/ in front!!!
sudo BLOCKCNT=<path_to_bpftrace_reset_script> ./benchmark.sh run zenfs <ZNS_device> <ZNS_device>
sudo ./benchmark.sh clean zenfs <ZNS_device> <ZNS_device> # no dev!
```

# Utils
This directory contains various setup scripts that can aid in setting up the targets to benchmark.
For example, automatic resetting of ZNS SSDs, creating file systems on top of ZNS and mounting file systems.
This is al inline with the methods used in the TropoDB paper. There are scripts for TropoDB, ZenFS and F2FS.
To get more information on each script, help can be printed by not supplying an argument.

