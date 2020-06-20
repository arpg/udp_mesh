#!/usr/bin/python2
'''
PriorityQueue with a key function for messages
From https://stackoverflow.com/questions/407734/a-generic-priority-queue-for-python
'''

from Queue import PriorityQueue

class KeyHeap(PriorityQueue):
    def __init__(self, key, maxsize=0):            
        PriorityQueue.__init__(self, maxsize)
        self.key = key

    def put(self, x):
        PriorityQueue.put(self, (self.key(x), x))

    def get(self):
        return PriorityQueue.get(self)[1]
