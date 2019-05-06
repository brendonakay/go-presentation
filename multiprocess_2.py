import csv
import multiprocessing as mp
import os
import queue as q
import re
import sys
import time
from datetime import datetime

def print_edited_line(line):
    line[0] = datetime.strptime(line[0], '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d')
    line[1] = re.sub('(?i)([A-Z]+) ([A-Z]+)', r'\2, \1', line[1])
    line[2] = line[2].upper()

    # print(f'{os.getpid()}: ' + '|'.join(line))
    print('|'.join(line))
    # sys.stdout.flush()


def consume(queue, lock):
    while True:
        try:
            with lock:
                line = queue.get(block=False)
                print_edited_line(line)
        except q.Empty:
            break


def transform_csv():
    with open(sys.argv[1], 'r') as file_obj:
        reader = csv.reader(file_obj, delimiter='|')

        num_procs = 4
        if len(sys.argv) >= 3:
            num_procs = int(sys.argv[2])

        lock = mp.Lock()
        queue = mp.Queue()

        processes = []
        for i in range(0, num_procs):
            p = mp.Process(target=consume, args=(queue, lock))
            p.daemon = True
            processes.append(p)
            p.start()

        # Remove header
        _ = next(reader)

        for line in reader:
            queue.put(line)

        for p in processes:
            p.join()



if __name__ == '__main__':
    start_time = time.time()
    transform_csv()
    end_time = time.time() - start_time
    sys.stderr.write(f"\n{end_time} seconds\n")
