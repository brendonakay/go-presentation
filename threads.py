import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


def print_edited_line(line):
    line[0] = datetime.strptime(line[0], '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d')
    line[1] = re.sub('(?i)([A-Z]+) ([A-Z]+)', r'\2, \1', line[1])
    line[2] = line[2].upper()

    print('|'.join(line))


def transform_csv():
    with open(sys.argv[1], 'r') as file_obj:
        reader = csv.reader(file_obj, delimiter='|')

        # Remove header
        _ = next(reader)

        thread_local = threading.local()
        thread_local.reader = reader

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(print_edited_line, reader)


if __name__ == '__main__':
    start_time = time.time()
    transform_csv()
    end_time = time.time() - start_time
    print(f"\n{end_time} seconds")
