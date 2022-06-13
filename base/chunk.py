# -*- coding: utf-8 -*-
sentinel = object()


class Chunk:
    def __init__(self, first, iterator, batch):
        self.next = first
        self.iterator = iterator
        self.batch = batch

    def __iter__(self):
        return self

    def __next__(self):
        if self.next is sentinel:
            raise StopIteration
        item = self.next
        self.batch -= 1
        if self.batch > 0:
            self.next = next(self.iterator, sentinel)
        else:
            self.next = sentinel
        return item


def chunks(iterable, batch):
    iterator = iter(iterable)
    while True:
        first = next(iterator, sentinel)
        if first is sentinel:
            return
        yield Chunk(first, iterator, batch)
