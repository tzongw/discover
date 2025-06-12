# -*- coding: utf-8 -*-
import sys
import subprocess
from dataclasses import dataclass


@dataclass
class ProcessInfo:
    name: str
    pid: int


def rollover_restart(running):
    if not running:
        raise RuntimeError('no service processes running')
    if len(running) == 1:
        raise RuntimeError('one process running can not rollover')
    print(f'total {len(running)} to restart')
    for info in running:
        print('> ', info.name)
    start_index = 0
    batch = 1
    total = len(running)
    while start_index < total:
        actions = ['y', 'NO', '<batch>']
        if start_index:
            actions += ['REST']
        prompt = '/'.join(actions)
        while True:
            print(f'restart {start_index}/{total}, continue? ({prompt})')
            answer = sys.stdin.readline().strip()
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
        if answer == 'REST':  # restart all rest
            pids = [str(info.pid) for info in running[start_index:]]
            text = subprocess.check_output(['kill', '-TERM'] + pids, text=True)
            print(f'kill output: {text}')
            exit(0)
        if answer == 'NO':
            print('user aborted')
            exit(0)
        assert answer == 'y'
        names = []
        stop_index = min(start_index + batch, total)
        for index in range(start_index, stop_index):
            info = running[index]
            names.append(info.name)
            print(f'restarting {index + 1}/{len(running)}')
        text = subprocess.check_output(['supervisorctl', 'restart'] + names, text=True)
        print(f'supervisor output:')
        print(text)
        start_index = stop_index


def main():
    if len(sys.argv) <= 1:
        raise RuntimeError('usage: python deploy.py <service>')
    service = sys.argv[1]
    text = subprocess.check_output(['supervisorctl', 'status', f'{service}:*'], text=True)
    running = []
    for line in text.strip().split('\n'):
        name, status, _, pid, *_ = line.split()
        info = ProcessInfo(name=name, pid=int(pid.strip(',')))
        if status != 'RUNNING':
            raise RuntimeError(f'process not running: {line}')
        running.append(info)
    rollover_restart(running)


if __name__ == '__main__':
    main()
