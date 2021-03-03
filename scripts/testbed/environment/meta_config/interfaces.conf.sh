#!/bin/bash

IP_ADDR_CIDR="$1"
GATEWAY_ADDR="$2"

if [ $# -ne 2 ]; then
	echo "Usage: $0 <IP_ADDR_CIDR> <GATEWAY_ADDR>"
	echo "For example: $0 172.16.0.100/24 172.16.0.160"
	exit 1
fi

cat <<EOF
[Match]
Name=enp1s0

[Network]
Address=${IP_ADDR_CIDR}
Gateway=${GATEWAY_ADDR}
DNS=152.3.140.31
DNS=152.3.140.1
EOF
