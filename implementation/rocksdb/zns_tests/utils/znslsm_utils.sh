#!/bin/bash
set -e

print_help() {
    echo "Options:"
    echo "  setup: Setup environment for testing"
	echo "	create: alias for setup"
	echo "	destroy: Destroy all SPDK bindings and return control to host"
    echo ""
}

if [[ $# -eq 0 ]] ; then
    echo ""
    echo "Not enough arguments given, please provide the function you want to use..."
    echo ""
	print_help
	exit 1
fi

setup() {
    if [[ $# -lt 1 ]] ; then
        echo ""
        echo "Not enough arguments given, please provide a valid device TRID "
		echo "Trid can be retrieved by looking at 'pcie' with nvme-cli and the command list-subsys"
        echo ""
		exit 1
    fi
	if [[ -f $SPDK_DIR/scripts/setup.sh ]]; then
		# /dev/nvme4n1
		dev=$1
		reset="$SPDK_DIR/scripts/setup.sh reset"
		$reset
		export PCI_ALLOWED=$1
		$SPDK_DIR/scripts/setup.sh
	else
		echo ""
		echo "Please provide a valid device and set the SPDK_DIR environment variable."
		echo ""
		exit 1
	fi
}

destroy() {
	if [[ -f $SPDK_DIR/scripts/setup.sh ]]; then
		reset="$SPDK_DIR/scripts/setup.sh reset"
		$reset
	else
		echo ""
		echo "Please set the SPDK_DIR environment variable."
		echo ""
		exit 1
	fi
}

case $1 in 
    "setup")
        shift
        setup $*
        exit $?
    ;;
	"create")
        shift
        setup $*
        exit $?
    ;;
	"destroy")
		shift
		destroy
		exit $?
	;;
    *)
        echo "Unknown command..."
        echo ""
        print_help
        exit 1
    ;;
esac
