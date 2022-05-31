#!/bin/env bash
set -e

DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the function you want to use..."
    echo ""
    exit 1
fi

TARGET=$1
OPT=$2

source setup_bench.sh

run_bench