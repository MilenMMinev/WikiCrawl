from queue import Queue

class LinkQueue:
    """
    A Thread-safe queue of unique elements.
    """

    def __init__(self, maxsize = 10000):
        self.items = []
        self.queue = Queue(maxsize = maxsize)

    def __str__(self):
        return self.items.__str__()

    def put_all(self, items):
        for i in items:
            self.put(i)

    def put(self, item):
        if item not in self.items:
            self.queue.put(item)
            self.items.append(item)

    def pop(self):
        item = self.queue.get(block=True)
        return item

    def size(self):
        return self.queue.qsize()
