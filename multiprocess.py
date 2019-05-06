import csv
import multiprocessing as mp
import os
import re
import sys
import time
from datetime import datetime
from functools import partial

def print_edited_line(lock, line):
    # name = multiprocessing.current_process().name
    line[0] = datetime.strptime(line[0], '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d')
    line[1] = re.sub('(?i)([A-Z]+) ([A-Z]+)', r'\2, \1', line[1])
    line[2] = line[2].upper()

    with lock:
        # print(f'{os.getpid()}: ' + '|'.join(line))
        print('|'.join(line))
        sys.stdout.flush()


def transform_csv():
    with open(sys.argv[1], 'r') as file_obj:
        reader = csv.reader(file_obj, delimiter='|')

        # Remove header
        _ = next(reader)

        lock = mp.Manager().Lock()
        p = partial(print_edited_line, lock)
        with mp.Pool() as pool:
            pool.map(p, reader)


if __name__ == '__main__':
    start_time = time.time()
    transform_csv()
    end_time = time.time() - start_time
    sys.stderr.write(f"\n{end_time} seconds\n")
