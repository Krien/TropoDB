#!/bin/bash
set -e

if ! command -v nvme &> /dev/null
then
    echo ""
    echo "Please add nvme-cli to your path"
    echo ""
    exit 1
fi 

if ! command -v mkfs.f2fs &> /dev/null
then
    echo ""
    echo "Please add mkfs.f2fs to your path"
    echo ""
    exit 1
fi 

print_help() {
    echo "Options:"
    echo "  mkfs: Create filesystem"
    echo "  setup: Setup environment for testing"
    echo "  create: Setup + mkfs"
    echo "  destroy: Unmounts the filesystem and resets the device"
    echo ""
}

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the function you want to use..."
    echo ""
    print_help
    exit 1
fi

mkf_f2fs() {
    # /mnt/f2fs nvme4n1 nvme1n1
    if [[ $# -lt 3 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide mnt path, device name 1, device name 2 "
        echo ""
        exit 1
    fi
    mnt=$1
    devzns=$2
    devnvme=$3
    # F2FS does not properly reset the device, therefore we do it instead
    reset_zones_c="nvme zns reset-zone /dev/$devzns -a"
    $reset_zones_c
    # FS
    mkfs_c="mkfs.f2fs -f -m -c /dev/$devzns /dev/$devnvme"
    $mkfs_c
    mount_c="mount -t f2fs /dev/$devnvme $mnt"
    $mount_c
}

setup() {
    # nvme4n1
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a device name such as nvme2n2..."
        echo ""
        exit 1
    fi
    echo mq-deadline | sudo tee "/sys/block/$1/queue/scheduler"
}

create() {
    if [[ $# -lt 3 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide mnt path, device name 1, device name 2 "
        echo ""
        exit 1
    fi
    mnt=$1
    devzns=$2
    devnvme=$3
    setup $devzns
    mkf_f2fs $mnt $devzns $devnvme
}

destroy() {
    if [[ $# -lt 2 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a mount point and a device name \
        such as nvme2n2..."
        echo ""
        exit 1
    fi
    mnt=$1
    devzns=$2
    umount $mnt
    reset_zones="nvme zns reset-zone /dev/$devzns -a"
    $reset_zones
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
    "destroy")
        shift
        destroy $*
        exit $?
    ;;
    "create")
        shift
        create $*
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