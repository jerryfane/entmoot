#!/usr/bin/env python3
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import threading
import subprocess
import tempfile
import time

BIN = str(Path(sys.argv[1]).resolve())
root = Path(tempfile.mkdtemp(prefix='entmoot-libp2p-canary-'))
nodes = {}
processes = {}
logs = []

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

for name in ['a', 'b', 'c']:
    data = root / name
    data.mkdir()
    nodes[name] = [BIN, '-data', str(data), '-identity', str(data / 'identity.json'), '-listen-port', str(int(os.environ.get('ENTMOOT_CANARY_PORT_' + name.upper(), '0')) or port())]

def run(name, *args, check=True):
    result = subprocess.run(nodes[name] + list(args), text=True, capture_output=True, timeout=40)
    if check and result.returncode:
        raise RuntimeError(f'{name} {args}: {result.returncode}\n{result.stdout}\n{result.stderr}')
    return result

def obj(name, *args):
    return json.loads(run(name, *args).stdout)

def wait_for(label, predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.4)
    raise AssertionError('deadline: ' + label)

def start(name):
    handle = open(root / f'{name}-{time.time_ns()}.log', 'w')
    logs.append(handle)
    process = subprocess.Popen(nodes[name] + ['serve'], stdout=handle, stderr=subprocess.STDOUT)
    processes[name] = process
    def running():
        if process.poll() is not None:
            raise RuntimeError(f'{name} daemon exited {process.returncode}')
        response = run(name, 'info')
        return json.loads(response.stdout).get('running') is True
    wait_for(name + ' daemon ready', running)
    concurrent_info(name)

def concurrent_info(name):
    # A serving root may have an operational database locked by its owner.
    # Readiness must not checkpoint it. The transaction makes the old failure
    # deterministic; the barrier also exercises simultaneous journal readers.
    with sqlite3.connect(root / name / 'readiness.sqlite') as connection:
        connection.execute('CREATE TABLE IF NOT EXISTS readiness(value TEXT)')
        connection.commit()
        connection.execute('BEGIN EXCLUSIVE')
        connection.execute("INSERT INTO readiness VALUES ('uncommitted')")
        barrier = threading.Barrier(24, timeout=30)
        def info(_):
            barrier.wait()
            response = obj(name, 'info')
            assert response['running'] is True, response
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
            list(pool.map(info, range(24)))
        connection.rollback()
    print(f'PASS {name}: serving daemon and 24 simultaneous read-only info calls', flush=True)

def stop(name):
    process = processes.pop(name, None)
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise AssertionError(name + ' failed graceful shutdown')

def contents(name, group):
    response = run(name, 'query', '-group', group, '-topic', 'canary/#', '-limit', '1000')
    return [json.loads(line)['content'] for line in response.stdout.splitlines() if line.strip()]

def received(name, group, text):
    return text in contents(name, group)

def assert_unjoined(name, group):
    response = run(name, 'query', '-group', group, '-topic', 'canary/#', check=False)
    assert response.returncode == 3 and not response.stdout.strip(), response

def publish(name, group, text):
    run(name, 'publish', '-group', group, '-topic', 'canary/expanded', '-content', text)

def invite(name, group, founder_peer):
    public = obj(name, 'info')['entmoot_pubkey']
    address = f'/ip4/127.0.0.1/tcp/{nodes["a"][-1]}/p2p/{founder_peer}'
    data = run('a', 'invite', 'create', '-group', group, '-target-pubkey', public, '-bootstrap', address).stdout
    path = root / f'{name}-{len(list(root.glob("invite-*")))}.json'
    path = root / ('invite-' + path.name)
    path.write_text(data)
    return str(path)

success = False
try:
    for name in nodes:
        obj(name, '-allow-new-identity', 'info')
    g1 = obj('a', 'group', 'create', '-name', 'expanded-one', '-policy', 'none', '-json')
    g2 = obj('a', 'group', 'create', '-name', 'expanded-two', '-policy', 'none', '-json')
    group1, group2 = g1['group_id'], g2['group_id']
    start('a')
    b1 = invite('b', group1, g1['founder']['peer_id'])
    b2 = invite('b', group2, g2['founder']['peer_id'])
    run('b', 'join', b1, b2)
    c1 = invite('c', group1, g1['founder']['peer_id'])
    run('c', 'join', c1)
    start('b')
    start('c')
    print('READY: three daemons; A/B in two groups; C only in group one', flush=True)
    # Warm-up messages establish observed delivery, rather than treating TCP or
    # daemon readiness as proof that GossipSub subscriptions have propagated.
    for group, members in [(group1, ['b', 'c']), (group2, ['b'])]:
        deadline = time.monotonic() + 45
        while True:
            publish('a', group, 'warmup-' + group)
            if all(received(name, group, 'warmup-' + group) for name in members):
                break
            if time.monotonic() >= deadline:
                raise AssertionError('initial member delivery failed')
            time.sleep(2)
    print('PASS initial delivery: both groups and both remote members', flush=True)
    tail_path = root / 'tail.log'
    tail_log = open(tail_path, 'w')
    logs.append(tail_log)
    processes['tail'] = subprocess.Popen(nodes['b'] + ['tail', '-group', group1, '-topic', 'canary/#', '-n', '-1'], stdout=tail_log, stderr=subprocess.STDOUT)
    wait_for('tail historical catchup', lambda: 'warmup-' + group1 in tail_path.read_text())
    publish('a', group1, 'group-one-live')
    publish('b', group2, 'group-two-member-published')
    wait_for('group one live fanout', lambda: all(received(n, group1, 'group-one-live') for n in ['b', 'c']))
    wait_for('group two member publication', lambda: received('a', group2, 'group-two-member-published'))
    assert 'group-two-member-published' not in contents('c', group1)
    assert 'group-one-live' not in contents('b', group2)
    assert_unjoined('c', group2)
    print('PASS live fanout/member publishing and two-group storage separation', flush=True)
    wait_for('tail live subscription', lambda: 'group-one-live' in tail_path.read_text())
    stop('tail')
    print('PASS tail historical catchup and live subscription', flush=True)
    stop('b')
    publish('a', group1, 'offline-group-one')
    publish('a', group2, 'offline-group-two')
    start('b')
    wait_for('two-group offline catchup', lambda: received('b', group1, 'offline-group-one') and received('b', group2, 'offline-group-two'))
    print('PASS offline member restart/catchup across both groups', flush=True)
    for name in ['c', 'b', 'a']:
        stop(name)
    for name in ['a', 'b', 'c']:
        start(name)
    assert received('b', group1, 'offline-group-one') and received('b', group2, 'offline-group-two')
    publish('a', group1, 'after-full-restart-one')
    publish('a', group2, 'after-full-restart-two')
    wait_for('all-daemon restart delivery', lambda: all(received(n, group1, 'after-full-restart-one') for n in ['b', 'c']) and received('b', group2, 'after-full-restart-two'))
    assert_unjoined('c', group2)
    print('PASS full three-daemon restart, persisted membership/peers/history, both-group delivery', flush=True)
    success = True
    print('LIBP2P CANARY PASSED: 3 daemons; 2 groups; fresh joins, live fanout, member publishing, group separation, offline/full restart catchup', flush=True)
finally:
    for name in list(processes):
        stop(name)
    for handle in logs:
        handle.close()
    if success:
        import shutil
        shutil.rmtree(root)
    else:
        print('FAILED fixture retained at', root, flush=True)
        for path in root.glob('*.log'):
            print('LOG', path, path.read_text()[-6000:], flush=True)
