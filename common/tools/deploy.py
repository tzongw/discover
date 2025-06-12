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
    for index, info in enumerate(running):
        if not yes:
            print(f'restart {info.name} {index + 1}/{len(running)}, continue? (YES/Y/n)')
            answer = sys.stdin.readline().strip()
            if answer == 'YES':
                yes = True
            elif answer != 'Y':
                print('user aborted')
                exit(0)
        print(f'restarting {info.name} {index + 1}/{len(running)}')
        text = subprocess.check_output(['supervisorctl', 'restart', info.name], text=True)
        print(f'done {info.name} {index + 1}/{len(running)}, output: {text}')


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
