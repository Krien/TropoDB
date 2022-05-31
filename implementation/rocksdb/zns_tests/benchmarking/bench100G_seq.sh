#!/bin/env bash
set -e

DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the ZNS SSDs trid..."
    echo ""
fi

trid=$1
# we want to go to 100GB (so 100 * 1024 * 1024 * (512 + 512) will do)
num_of_kvpairs=$((100*1024*1024))
ksize=512
vsize=512
benchmark=fillseq

./db_bench --use_zns=true \
--db=$trid \
--num=$num_of_kvpairs \
--compression_type=None \
--value_size=$ksize \ 
--key_size=$vsize \
--histogram \
--benchmarks=$benchmark \
--use_direct_io_for_flush_and_compaction \
--statistics 