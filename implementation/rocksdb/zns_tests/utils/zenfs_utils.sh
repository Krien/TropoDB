#!/bin/bash
#based on https://github.com/westerndigitalcorporation/zenfs/blob/master/tests/utils/0010_mkfs.sh
set -e

if [[ ! -f $ZENFS_DIR/zenfs ]]; then
    echo ""
	echo "Please set the ZEN_FS environment variable to the zenfs utils directory."
	echo ""
	exit 1
fi

print_help() {
    echo "Options:"
    echo "  mkfs: Create filesystem"
    echo "  setup: Setup environment for testing"
    echo ""
}

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the function you want to use..."
    echo ""
    print_help
    exit 1
fi

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
        echo "Not enough arguments given, please provide a device name \
        such as nvme2n2..."
        echo ""
    fi
    echo deadline | sudo tee "/sys/block/$1/queue/scheduler"
}

destroy() {
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a device name \
        such as nvme2n2..."
        echo ""
        exit 1
    fi
    devzns=$1
    reset_zones="nvme zns reset-zone /dev/$devzns -a"
    $reset_zones   
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
    "destroy")
        shift
        destroy $*
        exit $?
    ;;
    *)
        echo "Unknown command..."
        echo ""
        print_help
        exit 1
    ;;
esac
