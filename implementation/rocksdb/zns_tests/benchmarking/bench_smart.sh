#!/bin/bash
set -e

output_smartlog() {
    if [ $# -lt 2 ]; then
        echo "Please provide in order:"
        echo "  a device such as nvme6n1"
        echo "  an output file (absolute path please)"
        return 1
    fi
    nvme smart-log -o json "/dev/$1" > "./$2.json"
    return 0
}

DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR
cd ..
cd output


