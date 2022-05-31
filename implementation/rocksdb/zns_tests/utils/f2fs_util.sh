#!/bin/bash
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

mkf_f2fs() {
    if [[ $# -lt 3 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide mnt path, device name 1, device name 2 "
        echo ""
    fi
    # /mnt/f2fs /dev/nvme4n1 /dev/nvme1n1
    mnt=$1
    devzns=$2
    devnvme=$3
    mkfs.f2fs -f -m -c $devzns $devnvme
    mount -t f2fs $devnvme $mnt
}

setup() {
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a device name such as nvme2n2..."
        echo ""
    fi
    echo mq-deadline | sudo tee "/sys/block/$1/queue/scheduler"
}

case $1 in 
    "mkfs")
        shift
        mkf_f2fs $*
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

# /sys/kernel/debug/tracing/events/f2fs/f2fs_issue_reset_zone