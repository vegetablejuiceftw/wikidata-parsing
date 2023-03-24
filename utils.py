import multiprocessing as mp
import gzip, os, bz2
from dataclasses import dataclass, field
from time import sleep
from typing import Callable

from tqdm.auto import tqdm

chunk_size = 1024 * 1024 * 4


@dataclass
class FileChunkReader:
    file_path: str
    queue: mp.Queue
    fraction: float = None

    def start(self):
        if self.file_path.endswith('.gz'):
            with gzip.open(self.file_path, 'rb', encoding=None, errors=None, newline=None, compresslevel=1) as file:
                self.loop(file)
        if self.file_path.endswith('.bz2'):
            with bz2.open(self.file_path, 'rb', encoding=None, errors=None, newline=None, compresslevel=1) as file:
                file: bz2.BZ2File
                file.fileobj = file._fp  # noqa
                self.loop(file)

    def arrive(self, file):
        pass

    def depart(self, file):
        # for _ in range(128):
        self.queue.put(None)

    def loop(self, file):
        self.arrive(file)

        for chunk in self.read_chunks(file):
            while self.queue.qsize() > 64:
                sleep(0.1)
            self.queue.put(chunk)

        self.depart(file)

    def clean(self, chunk: bytes):
        pass

    def read_chunks(self, file):
        start_pos, file_size = 0, os.path.getsize(self.file_path)
        if self.fraction is not None:
            file_size = int(file_size * self.fraction)
        pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc='Processing chunks', mininterval=0.5)

        buffer = b''
        while start_pos < file_size:
            result = file.read(chunk_size)
            new_start_pos = file.fileobj.tell()  # noqa
            change = new_start_pos - start_pos
            pbar.desc = f"Processing chunks[queue size:{self.queue.qsize()}]"
            pbar.update(change)
            start_pos = new_start_pos

            head, *tail = result.rsplit(b'\n', 1)
            tail = tail[0] if tail else b''
            head = buffer + head
            buffer = tail
            yield head


class FileChunkReaderGZip(FileChunkReader):

    def arrive(self, file):
        _garbage = file.readline()  # drop the initial b"[\n"


def wrapper(queue_input: mp.Queue, queue_output: mp.Queue, function: Callable, index=None):
    for item in iter(queue_input.get, None):
        result = function(item)
        # print(len(result))
        queue_output.put(result)
        while queue_output.qsize() > 64:
            sleep(0.1)

    queue_input.put(None)
    queue_output.put(None)


import orjson


def parse_wikidata_simple(chunk: bytes):
    chunk = chunk.strip(b'\n]')
    chunk = chunk.strip(b',')
    chunk = b"[" + chunk + b"]"
    rows = orjson.loads(chunk)
    return rows[0]


def parse_wikidata(chunk: bytes):
    chunk = chunk.strip(b'\n]')
    chunk = chunk.strip(b',')
    chunk = b"[" + chunk + b"]"

    data = []
    for row in orjson.loads(chunk):
        label = row['labels'].get('en', {}).get('value')
        if not label:
            continue
        if not row['type'] == 'item':
            continue

        sitelink = row['sitelinks'].get('enwiki', {})
        sitelink, sitebadges = sitelink.get('title'), tuple(sitelink.get('badges', tuple()))
        sitelink_count = len(row['sitelinks'])
        label_count = len(row['labels'])
        description = row['descriptions'].get('en', {}).get('value', '')
        aliases = tuple(
            e['value']
            for e in row['aliases'].get('en', [])
        )

        data.append(dict(
            id=row['id'],
            label=label,
            sitelink=sitelink,
            sitebadges=sitebadges,
            sitelink_count=sitelink_count,
            label_count=label_count,
            description=description,
            aliases=aliases,
        ))

    return tuple(data)


import lance
import pandas as pd


def dump_lance(file_path, queue_input: mp.Queue, lock):
    result = []
    for item in iter(queue_input.get, None):
        result.extend(item)
        if len(result) > 1024 * 1024:
            if queue_input.qsize() > 100:
                print(f"Dump queue full: {queue_input.qsize()}")
            print("saving")
            lance.write_dataset(pd.DataFrame(result), file_path, mode='append')
            result = []
    if result:
        print("saving last")
        lance.write_dataset(pd.DataFrame(result), file_path, mode='append')

    queue_input.put(None)


if __name__ == '__main__':
    manager = mp.Manager()
    lock = manager.Lock()

    file_path = '/home/fred/Downloads/wikidata-20220103-all.json.gz'
    # file_path = '/home/fred/Downloads/latest-all.json.bz2'
    file_path_output = 'wikidata-20220103.lance'

    workers = []
    queue_input = manager.Queue()
    queue_output = manager.Queue()

    reader = FileChunkReaderGZip(file_path=file_path, queue=queue_input, fraction=0.1)
    p_reader = mp.Process(target=reader.start, name="Reader")
    p_reader.start()
    workers.append(p_reader)

    num_processes = max(1, mp.cpu_count() - 2)  # Use available CPU cores
    for i in range(num_processes):
        p_worker = mp.Process(target=wrapper, args=(reader.queue, queue_output, parse_wikidata, i), name=f'Parser {i}')
        p_worker.start()
        workers.append(p_worker)

    for i in range(1):
        p_dumper = mp.Process(target=dump_lance, args=(file_path_output, queue_output, lock), name=f'Dumper {i}')
        p_dumper.start()
        workers.append(p_dumper)

    print("Running")
    for worker in workers:
        worker.join()
        print(f"closed {worker.name}")
    print("finished")
