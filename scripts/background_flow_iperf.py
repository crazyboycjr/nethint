#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python39"
import subprocess
import threading
import shlex
import time
import argparse
import random

HOSTS_ALL = [
'192.168.211.2',
'192.168.211.34',
'192.168.211.66',
'192.168.211.130',
'192.168.211.162',
'192.168.211.194',
]

hosts = HOSTS_ALL
hosts_mask = [1] * len(hosts)

def get_avail_by_mask(hosts, mask):
    return [x[1] for x in filter(lambda x: x[0] != 0, zip(mask, hosts))]


hosts_avail = get_avail_by_mask(hosts, hosts_mask)


def run_task(cmd):
    #cmd_snip = shlex.split(cmd + " i am " + str(tid))
    cmd_snip = shlex.split(cmd)
    p = subprocess.Popen(cmd_snip, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    #print 'out:', out
    #print 'err:', err
    #print 'rc:', p.returncode
    return out, err

'''
iperf -c 192.168.211.3 -t 10 -y C,D -i 1
20210416192514,192.168.211.35,43074,192.168.211.3,5001,3,2.0-3.0,1236926464,9895411712
'''
def parse_iperf_output(text):

    ts, remote_ip, remote_port, local_ip, local_port, _, dura, bytes_sent, speed_bits = text.decode().split(',')

    speed_gbps = float(speed_bits) / 1e9
    # print('local_ip={}, remote_ip={}, bw={}'.format(local_ip, remote_ip, bw))
    return local_ip, remote_ip, speed_gbps

def receiver_task(cmd):
    out, err = run_task(cmd)
    local_ip, remote_ip, bw = parse_iperf_output(out)
    return local_ip, remote_ip, bw


def emit_pair(client, server, port, duration):
    server_log = '/tmp/iperf_flow_server_{}_{}_{}.txt'.format(client, server, port)
    client_log = '/tmp/iperf_flow_client_{}_{}_{}.txt'.format(client, server, port)
    cmd_s = 'iperf -s -P 1 -p {} -y C,D > {} &'.format(port, server_log)
    cmd_c = 'iperf -c {} -p {} -t {} -y C,D > {} &'.format(server, port, duration, client_log)
    return [cmd_s, cmd_c, 'cat {}'.format(server_log), 'cat {}'.format(client_log)]


def append_cmds(cmds, host, cmd):
    if host in cmds:
        cmds[host].append(cmd)
    else:
        cmds[host] = [cmd]

def ssh_submit(cmds, ths):
    for k, v in cmds.items():
        cmd_on_host = ';'.join(v + ['wait'])
        cmd = 'ssh -p 22 -o StrictHostKeyChecking=no {} "{}"'.format(k, cmd_on_host)
        print(cmd)
        ths.append(threading.Thread(target=run_task, args=(cmd, )))

def ssh_fetch(cmds):
    flows = []
    for [h, cmd] in cmds:
        ssh_cmd = 'ssh -p 22 -o StrictHostKeyChecking=no {} "{}"'.format(h, cmd)
        print(ssh_cmd)

        flows.append(receiver_task(ssh_cmd))
    return flows


def print_matrix(flows):

    def get_id_from_ip(ip, hosts):
        for i, h in enumerate(hosts):
            if h == ip: return i
        assert False, "no valid id found for ip: {}".format(ip)

    def print_bw_matrix(title, bw_mat):
        print(title)

        matrix = []
        matrix.append(['src\dst'] + hosts_avail)
        for i in range(len(bw_mat)):
            matrix.append([hosts_avail[i]] + bw_mat[i])

        s = [[str(e) for e in row] for row in matrix]
        lens = [max(map(len, col)) for col in zip(*s)]
        fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
        table = [fmt.format(*row) for row in s]
        print('\n'.join(table))

    rg = range(len(hosts_avail))
    bw_matrix = [[0 for _ in rg] for _ in rg]
    for f in flows:
        local_ip, remote_ip, bw = f
        src_id = get_id_from_ip(remote_ip, hosts_avail)
        dst_id = get_id_from_ip(local_ip, hosts_avail)
        assert src_id != dst_id, '({}, {}), ({}, {})'.format(remote_ip, src_id, local_ip, dst_id)
        bw_matrix[src_id][dst_id] = bw

    print_bw_matrix('Bandwidth Matrix', bw_matrix)


def main(args):

    ths_s = []
    ths_c = []
    workers = hosts_avail[:]

    cmds_s = {}
    cmds_c = {}

    fetch_cmds_s = []

    base_port = 18000

    def emit_flow(client, server):
        nonlocal base_port
        ret = emit_pair(client, server, base_port, args.duration)
        append_cmds(cmds_s, server, ret[0])
        append_cmds(cmds_c, client, ret[1])
        fetch_cmds_s.append([server, ret[2]])
        base_port += 1

    # random pick up

    n = len(workers)
    ranks = list(range(n))

    for _ in range(args.flows):
        random.shuffle(ranks)
        x = ranks[0]
        y = ranks[1]
        emit_flow(workers[x], workers[y])
        if args.bidirectional:
            emit_flow(workers[y], workers[x])

    ssh_submit(cmds_s, ths_s)
    ssh_submit(cmds_c, ths_c)

    for th in ths_s:
        th.start()
    time.sleep(2)
    for th in ths_c:
        th.start()
    for th in ths_s + ths_c:
        th.join()


    flows = ssh_fetch(fetch_cmds_s)
    if args.matrix:
        print_matrix(flows)
    else:
        print(flows)


def add_args(parser):
    # parser.add_argument('-n', '--num-competitors', type=int, default=1, help='specify the number of competitor flows')
    parser.add_argument('-D', '--duration', type=int, default=10, help='flow duration (in seconds)')
    parser.add_argument('-B', '--bidirectional', type=bool, action=argparse.BooleanOptionalAction, help='flow is bidirectional or not')
    parser.add_argument('-F', '--flows', type=int, default=1, help='the number of flows')
    parser.add_argument('-m', '--matrix', type=bool, action=argparse.BooleanOptionalAction, help='output the bandwidth matrix')


# parse args
parser = argparse.ArgumentParser(description="Bandwidth allocation test.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
add_args(parser)
args = parser.parse_args()

while True:
    main(args)
