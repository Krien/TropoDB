#!/bin/env bash
./db_bench --num=1000000 --compression_type=None --db="0000:00:08" --use_zns=true
