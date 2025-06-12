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
    yes = False
    start_index = 0
    batch = 1
    while start_index < len(running):
        if not yes:
            stop_index = min(start_index + batch, len(running))
            for index in range(start_index, stop_index):
                print(f'restart batch: {batch}, continue? (YES/Y/<batch>/n)')
            answer = sys.stdin.readline().strip()
            if answer == 'YES':
                yes = True
            if answer.isdigit():
                batch = int(answer)
                assert batch > 0
                stop_index = min(start_index + batch, len(running))
            elif answer != 'Y':
                print('user aborted')
                exit(0)
        names = []
        for index in range(start_index, stop_index):
            info = running[index]
            names.append(info.name)
            print(f'restarting {info.name} {index + 1}/{len(running)}')
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
    stopped = []
    for line in text.strip().split('\n'):
        name, status, _, pid, *_ = line.split()
        info = ProcessInfo(name=name, pid=int(pid.strip(',')))
        if status == 'RUNNING':
            running.append(info)
        elif status == 'STOPPED':
            stopped.append(info)
    rollover_restart(running)
    print(f'{service} restart all done')


if __name__ == '__main__':
    main()
