# -*- coding: utf-8 -*-
import os
import sys
import signal
import subprocess
from dataclasses import dataclass
from base.utils import try_flock


@dataclass
class ProcessInfo:
    name: str
    status: str
    pid: int = None


def rolling_update(running, all_yes):
    total = len(running)
    if total == 1:
        raise RuntimeError('one process can not rolling')
    print(f'total {total} to roll')
    for info in running:
        print('> ', info.name)
    start_index = 0
    batch = 1
    while start_index < total:
        actions = ['y', 'NO', '<batch>']
        prompt = '/'.join(actions)
        while True:
            print(f'restart {start_index + 1}/{total}, continue? ({prompt})')
            answer = 'y' if all_yes else sys.stdin.readline().strip()
            if answer.isdigit():
                n = int(answer)
                upperbound = total - max(start_index, 1)
                if not (1 <= n <= upperbound):
                    print(f'batch out of range: [1, {upperbound}]')
                    continue
                batch = n
                answer = 'y'
            if answer in actions and answer != '<batch>':
                break
            print('unrecognized action')
        if answer == 'NO':
            print('user aborted')
            exit(0)
        assert answer == 'y'
        names = []
        stop_index = min(start_index + batch, total)
        for index in range(start_index, stop_index):
            info = running[index]
            names.append(info.name)
            print(f'restarting {index + 1}/{total} ...')
        text = subprocess.check_output(['supervisorctl', 'restart'] + names, text=True)
        print(f'supervisor output:')
        print(text)
        start_index = stop_index


def migrating_update(running, idle, all_yes):
    total = len(running)
    if len(idle) < total:
        raise RuntimeError('not enough idle processes')
    print(f'total {total} to migrate')
    for r, i in zip(running, idle):
        print('> ', r.name, '>>', i.name)
    start_index = 0
    batch = 1
    while start_index < total:
        actions = ['y', 'NO', '<batch>']
        prompt = '/'.join(actions)
        while True:
            print(f'migrate {start_index + 1}/{total}, continue? ({prompt})')
            answer = 'y' if all_yes else sys.stdin.readline().strip()
            if answer.isdigit():
                n = int(answer)
                upperbound = total - start_index
                if not (1 <= n <= upperbound):
                    print(f'batch out of range: [1, {upperbound}]')
                    continue
                batch = n
                answer = 'y'
            if answer in actions and answer != '<batch>':
                break
            print('unrecognized action')
        if answer == 'NO':
            print('user aborted')
            exit(0)
        assert answer == 'y'
        pids = []
        idle_names = []
        stop_index = min(start_index + batch, total)
        for index in range(start_index, stop_index):
            pids.append(running[index].pid)
            idle_names.append(idle[index].name)
            print(f'migrating {index + 1}/{total} ...')
        text = subprocess.check_output(['supervisorctl', 'start'] + idle_names, text=True)
        print(f'supervisor output:')
        print(text)
        for pid in pids:
            os.kill(pid, signal.SIGHUP)
        start_index = stop_index


def deploy(service, all_yes):
    try:
        text = subprocess.check_output(['supervisorctl', 'status', f'{service}:*'], text=True)
    except subprocess.CalledProcessError as e:
        if e.returncode != 3:
            raise
        text = e.stdout
    print(f'supervisor output:')
    print(text)
    running = []
    idle = []
    other = []
    for line in text.strip().split('\n'):
        name, status, *rest = line.split()
        if status == 'RUNNING':
            pid = rest[1]
            info = ProcessInfo(name=name, status=status, pid=int(pid.strip(',')))
            running.append(info)
        else:
            info = ProcessInfo(name=name, status=status)
            if status in ['STOPPED', 'EXITED']:
                idle.append(info)
            else:
                other.append(info)
    if not running:
        raise RuntimeError('no process running')
    if idle:
        migrating_update(running, idle, all_yes)
    elif not other:
        rolling_update(running, all_yes)
    else:
        raise RuntimeError('no strategy match')


def main():
    if not (len(sys.argv) == 2 or len(sys.argv) == 3 and sys.argv[1] == '-y'):
        raise RuntimeError('usage: python deploy.py [-y] <service>')
    all_yes = len(sys.argv) == 3
    service = sys.argv[-1]
    with try_flock(f'deploy_{service}.lock'):
        deploy(service, all_yes)


if __name__ == '__main__':
    main()
