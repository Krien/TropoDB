#!/bin/env bash
# we want to go to 10GB (so 100.000.000 * (500 + 500) will do)
./db_bench --num=100000000 --compression_type=None --db="0000:00:08" --use_zns=true --value_size=500 --key_size=500 --benchmarks=fillseq,readseq
