#!/usr/bin/env bash

RDMA_IP_ADDR_CIDR="$1"

if [ $# -ne 1 ]; then
	echo "Usage: $0 <IP_ADDR_CIDR>"
	echo "For example: $0 1.1.1.1/24"
	exit 1
fi

cat <<EOF
#!/usr/bin/env bash

if [ \$# -ne 1 ]; then
	echo "Usage: \$0 <intf>"
	exit 1
fi

intf=\$1

sudo ip link set \$intf name rdma0
sudo ip link set rdma0 up
sudo ip addr add ${RDMA_IP_ADDR_CIDR} dev rdma0
sudo ip link set rdma0 mtu 8930
EOF
