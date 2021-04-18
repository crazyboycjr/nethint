#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python39"
import subprocess
import threading
import shlex
import time
import argparse
import random
import os
import fcntl
import select
from queue import Queue
from multiprocessing.pool import ThreadPool

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

ready_queue = Queue()
result_queue = Queue()

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


def run_task_sync(cmd):
    cmd_snip = shlex.split(cmd)
    p = subprocess.Popen(cmd_snip, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return out, err

def run_task(cmd):
    cmd_snip = shlex.split(cmd)
    p = subprocess.Popen(cmd_snip, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # set stdout nonblocking
    flags = fcntl.fcntl(p.stdout, fcntl.F_GETFL)
    fcntl.fcntl(p.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    fcntl.fcntl(p.stderr, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    poll = select.poll()
    poll.register(p.stdout, select.POLLIN)
    poll.register(p.stderr, select.POLLIN)

    fin = False
    while not fin:
        for (fd, ev) in poll.poll(1):
            if fd == p.stdout.fileno():
                msg = p.stdout.readline()
                assert msg == b'iperf_ready\n', 'unexpected msg: {}'.format(msg)
                fin = True
            else:
                errmsg = p.stderr.read()
                print('err:', errmsg)


    ready_queue.put(1)
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

    ts, remote_ip, remote_port, local_ip, local_port, _, dura, bytes_sent, speed_bits = text.decode().strip().split(',')

    speed_gbps = float(speed_bits) / 1e9
    # print('local_ip={}, remote_ip={}, bw={}'.format(local_ip, remote_ip, bw))
    return local_ip, remote_ip, speed_gbps

def fetch_task(cmd):
    # out, err = run_task_sync(cmd)
    cmd_snip = shlex.split(cmd)
    p = subprocess.Popen(cmd_snip, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # set stdout nonblocking
    flags = fcntl.fcntl(p.stdout, fcntl.F_GETFL)
    fcntl.fcntl(p.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    fcntl.fcntl(p.stderr, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    poll = select.poll()
    poll.register(p.stdout, select.POLLIN | select.POLLERR)
    poll.register(p.stderr, select.POLLIN | select.POLLERR)

    while p.poll() is None:
        for (fd, ev) in poll.poll(1):
            if fd == p.stdout.fileno():
                msg = p.stdout.readline()
                if len(msg) == 0:
                    continue
                print(msg)
                local_ip, remote_ip, bw = parse_iperf_output(msg)
                result_queue.put((local_ip, remote_ip, bw))
            else:
                errmsg = p.stderr.read()
                print(errmsg)
    # TODO(cjr): return average bw
    return local_ip, remote_ip, bw


def emit_pair(client, server, port, duration):
    server_log = '/tmp/iperf_flow_server_{}_{}_{}.txt'.format(client, server, port)
    client_log = '/tmp/iperf_flow_client_{}_{}_{}.txt'.format(client, server, port)
    cmd_s = 'rm -f {log}.pipe && mkfifo {log}.pipe && iperf -s -P 1 -p {} -i 1 -y C,D | tee {log} {log}.pipe &'.format(port, log=server_log)
    cmd_c = 'iperf -c {} -p {} -t {} -y C,D > {log} &'.format(server, port, duration, log=client_log)
    return [cmd_s, cmd_c, 'stdbuf -oL -eL cat {}.pipe'.format(server_log), 'cat {}'.format(client_log)]


def append_cmds(cmds, host, cmd):
    if host in cmds:
        cmds[host].append(cmd)
    else:
        cmds[host] = [cmd]

def ssh_submit(cmds, ths):
    for k, v in cmds.items():
        cmd_on_host = ';'.join(v + ['echo iperf_ready', 'wait'])
        cmd = 'ssh -p 22 -o StrictHostKeyChecking=no {} "{}"'.format(k, cmd_on_host)
        print(cmd)
        ths.append(threading.Thread(target=run_task, args=(cmd, )))


def streaming_print(batch_size):
    while True:
        flows = []
        for _ in range(batch_size):
            f = result_queue.get()
            if f is None:
                return
            flows.append(f)
        print_flows(flows)


def ssh_fetch(cmds):
    nflows = len(cmds)
    th = threading.Thread(target=streaming_print, args=(nflows, ))
    th.start()

    pool = ThreadPool(nflows)
    ssh_cmds = []
    for [h, cmd] in cmds:
        ssh_cmd = 'ssh -p 22 -o StrictHostKeyChecking=no {} "{}"'.format(h, cmd)
        print(ssh_cmd)
        ssh_cmds.append(ssh_cmd)

    flows = pool.map(fetch_task, ssh_cmds)
    # print('ssh_fetch done')
    result_queue.put(None)
    th.join()

    return flows


def print_flows(flows):

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

    def update_bw_mat(m, i, j, b):
        if m[i][j] == '0':
            m[i][j] = b
        else:
            m[i][j] += ',' + b

    if args.matrix:
        rg = range(len(hosts_avail))
        bw_matrix = [['0' for _ in rg] for _ in rg]
        for f in flows:
            local_ip, remote_ip, bw = f
            src_id = get_id_from_ip(remote_ip, hosts_avail)
            dst_id = get_id_from_ip(local_ip, hosts_avail)
            assert src_id != dst_id, '({}, {}), ({}, {})'.format(remote_ip, src_id, local_ip, dst_id)
            update_bw_mat(bw_matrix, src_id, dst_id, '{:.2f}'.format(bw))

        print_bw_matrix('Bandwidth Matrix', bw_matrix)
    else:
        print(flows)


base_port = 18000
port = base_port

def main():

    ths_s = []
    ths_c = []
    workers = hosts_avail[:]

    cmds_s = {}
    cmds_c = {}

    fetch_cmds_s = []


    def emit_flow(client, server):
        global port
        ret = emit_pair(client, server, port, args.duration)
        append_cmds(cmds_s, server, ret[0])
        append_cmds(cmds_c, client, ret[1])
        fetch_cmds_s.append([server, ret[2]])
        port += 1
        if port > 60000:
            port = base_port

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

    # wait for all servers ready
    for _ in range(len(cmds_s)):
        ready_queue.get()
    # print('server started')

    for th in ths_c:
        th.start()

    # wait for all clients ready
    flows = ssh_fetch(fetch_cmds_s)

    # join all threads
    for th in ths_s + ths_c:
        th.join()

    for _ in range(len(cmds_c)):
        ready_queue.get()

    # print('join done')

    print_flows(flows)


while True:
    main()
