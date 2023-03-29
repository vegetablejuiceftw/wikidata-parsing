import multiprocessing as mp
import gzip, os, bz2
from dataclasses import dataclass
from typing import Iterable, Callable

from tqdm.auto import tqdm

import tarfile


@dataclass
class FileChunkReader:
    file_path: str
    fraction: float = None
    chunk_size: int = 1024 * 1024 * 8

    def __iter__(self):
        file_size = os.path.getsize(self.file_path.split(':')[0])

        if 'tar.gz' in self.file_path:
            file_path, file_name = self.file_path.split(':')
            with tarfile.open(file_path, 'r:gz') as tar:  # bufsize
                file = tar.extractfile(file_name)  # noqa
                file.seek(0, os.SEEK_END)
                file_size = file.tell()
                file.seek(0)
                file.fileobj = file
                for c in self.loop(file, file_size):  # noqa
                    yield c

        elif self.file_path.endswith('.gz'):
            with gzip.open(self.file_path, 'rb', encoding=None, errors=None, newline=None, compresslevel=1) as file:
                for c in self.loop(file, file_size):  # noqa
                    yield c

        elif self.file_path.endswith('.bz2'):
            with bz2.open(self.file_path, 'rb', encoding=None, errors=None, newline=None, compresslevel=1) as file:
                file: bz2.BZ2File
                file.fileobj = file._fp  # noqa
                for c in self.loop(file, file_size):  # noqa
                    yield c

    def arrive(self, file):
        pass

    def depart(self, file):
        pass

    def loop(self, file, file_size):
        self.arrive(file)
        for chunk in self.read_chunks(file, file_size):
            yield chunk
        self.depart(file)

    def read_chunks(self, file, file_size):
        start_pos = 0
        if self.fraction is not None:
            file_size = int(file_size * self.fraction)
        pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc='Processing chunks', mininterval=0.5)

        buffer = b''
        while start_pos < file_size:
            result = file.read(self.chunk_size)
            new_start_pos = file.fileobj.tell()  # noqa
            change = new_start_pos - start_pos
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


import orjson
import pyarrow as pa

schema_wikidata = pa.schema([
    pa.field(name="id", type=pa.string()),
    pa.field(name="label", type=pa.string()),
    pa.field(name="sitelink", type=pa.string()),
    pa.field(name="sitebadges", type=pa.list_(pa.string())),
    pa.field(name="sitelink_count", type=pa.int64()),
    pa.field(name="label_count", type=pa.int64()),
    pa.field(name="description", type=pa.string()),
    pa.field(name="aliases", type=pa.list_(pa.string())),
])


def parse_wikidata_simple(chunk: bytes):
    chunk = chunk.strip(b'\n]')
    chunk = chunk.strip(b',')
    chunk = b"[" + chunk + b"]"
    rows = orjson.loads(chunk)
    return rows


def parse_wikidata(chunk: bytes):
    data = []
    for row in parse_wikidata_simple(chunk):
        label = row['labels'].get('en', {}).get('value')
        if not label:
            continue
        if not row['type'] == 'item':
            continue

        sitelink = row['sitelinks'].get('enwiki', {})
        sitelink, sitebadges = sitelink.get('title', ''), tuple(str(b) for b in sitelink.get('badges', tuple()))
        sitelink_count = len(row['sitelinks'])
        label_count = len(row['labels'])
        description = row['descriptions'].get('en', {}).get('value', '')
        aliases = tuple(
            str(e['value'])
            for e in row['aliases'].get('en', [])
        )

        data.append(dict(
            id=str(row['id']),
            label=str(label),
            sitelink=str(sitelink),
            sitebadges=sitebadges,
            sitelink_count=sitelink_count,
            label_count=label_count,
            description=str(description),
            aliases=aliases,
        ))

    return tuple(data)


schema_wikidata_5m_entity = pa.schema([
    pa.field(name="id", type=pa.string()),
    pa.field(name="label", type=pa.string()),
    pa.field(name="aliases", type=pa.list_(pa.string())),
])


def parse_wikidata_5m_entity(chunk: bytes):
    data = []
    for row in chunk.decode('utf-8').splitlines():
        idx, label, *aliases = row.split("\t")
        data.append(dict(
            id=idx,
            label=label,
            aliases=aliases,
        ))

    return tuple(data)


def combine_chunks(chunks, item_count=1024 * 1024):
    result = []
    for item in chunks:
        result.extend(item)
        if len(result) > item_count:
            yield result
            result = []
    if result:
        yield result


def run_function(iterator: Iterable, function: Callable, num_processes=None):
    num_processes = num_processes or max(1, mp.cpu_count())  # Use available CPU cores
    with mp.Pool(num_processes) as pool:
        for e in pool.imap_unordered(function, iterator):
            yield e


import lance
import pandas as pd


def load_data(
    reader: Iterable,
    schema: pa.Schema,
    parser: Callable,
    file_path_output: str,
):
    for chunk in combine_chunks(run_function(reader, parser)):
        lance.write_dataset(pd.DataFrame(chunk), file_path_output, schema=schema, mode='append')

    dataset = lance.dataset(file_path_output)
    print("total rows:", dataset.count_rows())


if __name__ == '__main__':
    load_data(
        reader=FileChunkReaderGZip(
            '.data/latest-all.json.gz',  # 27mb/s or 1h:17m
            # '.data/latest-all.json.bz2',  # 6mb/s
            # fraction=0.05,
        ),
        schema=schema_wikidata,
        parser=parse_wikidata,
        file_path_output='wikidata-latest.lance',
    )

    # load_data(
    #     reader=FileChunkReader('.data/wikidata5m_alias.tar.gz:wikidata5m_entity.txt'),
    #     schema=schema_wikidata_5m_entity,
    #     parser=parse_wikidata_5m_entity,
    #     file_path_output='wikidata-5m.lance',
    # )
