
# we want to go to 100GB (so 100 * 1024 * 1024 * (512 + 512) will do)
NUM=$((100*1024*1024))
KSIZE=512
VSIZE=512

case $TARGET in
    "f2fs")
        F2FS_ARGS="--db=/mnt/f2fs/db0 --wal_dir=/mnt/f2fs/wal0"
        DB_BENCH_ARGS="$DB_BENCH_ARGS $F2FS_ARGS"
    ;;
    "zenfs")
        ZENFS_ARGS="-fs_uri=zenfs://dev:$OPT"
        DB_BENCH_ARGS="$DB_BENCH_ARGS $ZENFS_ARGS"
    ;;
    "tropodb")
        TROPODB_ARGS="--use_tropodb=true --db=$OPT"
        DB_BENCH_ARGS="$DB_BENCH_ARGS $TROPODB_ARGS"
    ;;
    *)
        echo "This target is not known..."
        exit -1
    ;;
esac

run_bench() {
    ${DB_BENCH_DIR}/db_bench $DB_BENCH_ARGS \
    --num=$NUM \
    --compression_type=None \
    --value_size=$KSIZE \ 
    --key_size=$VSIZE \
    --histogram \
    --benchmarks=$BENCHMARKS
}
