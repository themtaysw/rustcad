#!/usr/bin/env bash
# Creates a virtual CAN interface. Safe to run repeatedly; needs to be re-run after reboot.
set -euo pipefail

iface="${1:-vcan0}"

sudo modprobe vcan

if ! ip link show "$iface" >/dev/null 2>&1; then
    sudo ip link add dev "$iface" type vcan
fi

sudo ip link set up "$iface"
ip -brief link show "$iface"
