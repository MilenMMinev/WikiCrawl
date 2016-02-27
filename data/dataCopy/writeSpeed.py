from os import listdir
from os.path import isfile, join
import time


def main():
    beginTime = time.time()
    beginCount = len(listdir())
    while True:
        createdCount = len(listdir()) - beginCount
        createdTime = time.time() - beginTime
        try:
            print('speed: {}'.format(createdCount / createdTime))
        except:
            print('except')
        time.sleep(1)

if __name__ == '__main__':
    main()