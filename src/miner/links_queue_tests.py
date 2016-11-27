import unittest
import time
from threading import Thread
from threading import enumerate

from random import shuffle

from links_queue import LinkQueue

class TestLinksQueue(unittest.TestCase):

    def put_items(self, count, item):
        for i in range(count):
            self.links.put(item)

    def pop_item(self):
        item = self.links.pop()

    def test_async_put_and_pop(self):
        self.links = LinkQueue()
        n_threads = 20
        threads = [Thread(target=self.put_items, args=(
            10, i), daemon=True) for i in range(n_threads*2)]

        threads += [Thread(target=self.pop_item,
                           daemon=True) for i in range(n_threads)]

        shuffle(threads)

        for t in threads:
            t.start()


        self.assertEqual(n_threads, self.links.size())

    def test_put_above_max_size(self):
        self.links = LinkQueue(maxsize=100)

        n_threads = 20
        threads = [Thread(target=self.put_items, args=(
            10, i), daemon=True) for i in range(n_threads)]

        for t in threads:
            t.start()


        self.assertEqual(20, self.links.size())


if __name__ == '__main__':
    unittest.main()
