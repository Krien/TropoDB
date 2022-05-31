#!/bin/bash
#based on https://github.com/westerndigitalcorporation/zenfs/blob/master/tests/utils/0010_mkfs.sh
set -e

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the function you want to use..."
    echo ""
fi

print_help() {
    echo "Options:"
    echo "  -mkfs: Create filesystem"
    echo "  -setup: Setup environment for testing"
    echo ""
}

mkf_zenfs() {
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide device name"
        echo ""
    fi
    # /dev/nvme1n1 /tmp/zenfs-aux
    rm -rf /tmp/zenfs-aux
    $ZENFS_DIR/zenfs mkfs -zbd=$1 -aux_path=/tmp/zenfs-aux
}

setup() {
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a device name such as nvme2n2..."
        echo ""
    fi
    echo deadline | sudo tee "/sys/block/$1/queue/scheduler"
}

case $1 in 
    "mkfs")
        shift
        mkf_zenfs $*
        exit $?
    ;;
    "setup")
        shift
        setup $*
        exit $?
    ;;
    *)
        echo "Unknown command..."
        echo ""
        print_help
        exit 1
    ;;
esac
