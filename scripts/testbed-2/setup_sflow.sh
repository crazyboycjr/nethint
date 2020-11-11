#!/bin/bash

if [ $USER != "root" ]; then
        echo "Please run this script as root"
        exit 1
fi

if [ $# -eq 1 ]; then
	COLLECTOR_IP=$1
else
	COLLECTOR_IP=127.0.0.1
fi

COLLECTOR_PORT=6343
AGENT_IP=enp24s0v0
HEADER_BYTES=128
SAMPLING_N=10000
POLLING_SECS=10
BRIDGE=ovs0

ovs-vsctl -- --id=@sflow create sflow agent=${AGENT_IP} \
    target="\"${COLLECTOR_IP}:${COLLECTOR_PORT}\"" header=${HEADER_BYTES} \
    sampling=${SAMPLING_N} polling=${POLLING_SECS} \
      -- set bridge ${BRIDGE} sflow=@sflow

ovs-vsctl list sflow

# to remove, use ovs-vsctl remove bridge $BRIDGE sflow <sFlow UUID>
