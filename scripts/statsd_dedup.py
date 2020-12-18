#!/usr/bin/env python

import collections
import enum
import sys

class State(enum.Enum):
    UNDETERMINED = 0
    WRITE = 1
    EXCLUDE = 2

class Metric(object):
    def __init__(self, line):
        self.line = line
        self.time, remainder = line.split(' ', maxsplit=1)
        remainder, self.type = remainder.rsplit('|', maxsplit=1)
        self.name, self.value = remainder.rsplit(':', maxsplit=1)

        self.state = State.UNDETERMINED

    def reduce(self, previous):
        if previous == None:
            self.state = State.WRITE
        elif self.value != previous.value:
            self.state = previous.state = State.WRITE
        elif previous.state == State.UNDETERMINED:
            previous.state = State.EXCLUDE

    def write(self):
        if self.state != State.EXCLUDE:
            print(self.line, end='')

q = collections.deque()
d = {}

for line in sys.stdin:
    metric = Metric(line)
    metric.reduce(d.get(metric.name))

    d[metric.name] = metric

    q.append(metric)
    while len(q) > 0 and q[0].state != State.UNDETERMINED:
        p = q.popleft().write()

while len(q) > 0:
    p = q.pop().write()
