import json
import multiprocessing as mp
import gzip, os, bz2
from dataclasses import dataclass
from typing import Iterable, Callable, List

from tqdm.auto import tqdm

import tarfile


def demap(mapping: dict):
    for k, v in mapping.items():
        if isinstance(v, dict):
            if k not in v.values():
                return mapping
        elif v and isinstance(v, list) and isinstance(v[0], dict):
            for v in v:
                if k not in v.values():
                    return mapping
        else:
            return mapping
    result = list(mapping.values())
    if all(isinstance(r, list) for r in result):
        result = sum(result, start=[])
    return result


def demap_recursively(item):
    if isinstance(item, list):
        return [demap_recursively(v) for v in item]
    if isinstance(item, dict):
        return demap({
            k: demap_recursively(v)
            for k, v in item.items()
        })
    return item


@dataclass
class FileChunkReader:
    file_path: str
    fraction: float = None
    chunk_size: int = 1024 * 1024 * 8
    pbar: bool = True

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
            # with os.popen(f"cat {self.file_path} | gzip -d") as file:
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

        pbar = (
            tqdm(total=file_size, unit='B', unit_scale=True, desc='Processing chunks', mininterval=0.5, position=0)
            if self.pbar else None
        )

        buffer = b''
        while start_pos < file_size:
            result = file.read(self.chunk_size)
            if not result:
                break
            if hasattr(file, "fileobj"):
                new_start_pos = file.fileobj.tell()  # noqa
                change = new_start_pos - start_pos
                if self.pbar:
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

import msgpack

def fix_chunk(chunk: bytes):
    chunk = chunk.strip(b'\n]')
    chunk = chunk.strip(b',')
    chunk = b"[" + chunk + b"]"
    return chunk


def parse_wikidata_simple(chunk: bytes):
    chunk = fix_chunk(chunk)
    rows = orjson.loads(chunk)
    return rows

import capnp

def reformat_data_value(data_value: dict):
    if not data_value:
        return data_value
    if isinstance(data_value['value'], dict):
        data_value['value']['type'] = data_value['type']
        data_value = data_value['value']
    data_value = {k.replace("-", ""): str(v) for k, v in data_value.items()}
    return data_value


def reformat_data_value(data_value: dict):
    if not data_value:
        return data_value
    if isinstance(data_value['value'], dict):
        data_value['value']['type'] = data_value['type']
        data_value = data_value['value']

    for k, v in tuple(data_value.items()):
        if "-" in k:
            del data_value[k]
        data_value[k.replace("-", "")] = str(v)

    # data_value = {k.replace("-", ""): str(v) for k, v in data_value.items()}
    return data_value


def reformat(claim: dict):

    claim['mainsnak']['datavalue'] = reformat_data_value(claim['mainsnak'].get('datavalue', {}))
    for snak in claim.get('qualifiers', []):
        # print(snak)
        snak['datavalue'] = reformat_data_value(snak.get('datavalue', {}))
    for reference in claim.get('references', []):
        reference['snaksorder'] = reference.pop('snaks-order')
        for snak in reference['snaks']:
            # print(snak)

            snak['datavalue'] = reformat_data_value(snak.get('datavalue', {}))
    return claim


def wikidata_demap(entity: dict):
    entity = {
        k: v if not isinstance(v, dict) else list(v.values())
        for k, v in entity.items()
    }
    entity['aliases'] = [v for arr in entity['aliases'] for v in arr]
    entity['claims'] = [v for arr in entity['claims'] for v in arr]
    for claim in entity['claims']:
        claim['qualifiersorder'] = claim.pop('qualifiers-order', [])

        if 'qualifiers' in claim:
            claim['qualifiers'] = [v for arr in claim['qualifiers'].values() for v in arr]

        if 'references' in claim:
            for ref in claim['references']:
                ref['snaks'] = [v for arr in ref['snaks'].values() for v in arr]
        reformat(claim)
    return entity


wikidata_full = capnp.load('wikidata-full.capnp')
def parse_wikidata_capnp(chunk: bytes):
    chunk = fix_chunk(chunk)
    rows = orjson.loads(chunk)
    # rows = delete_hash(rows)
    # rows = [{**row, 'claims': []} for row in rows]
    rows = [wikidata_demap(row) for row in rows]
    # 23-14

    # rows = [{**row, 'claims':  sum(row['claims'].values(), start=[])} for row in rows]
    # rows = [demap_recursively(row) for row in rows]  # TODO: SLOW
    # rows = [{**row, 'claims':  [reformat(claim) for claim in row['claims']]} for row in rows]
    # print(rows[0]['descriptions'])
    # return rows
    # message = wikidata_full.Entry.new_message(**rows[0])
    # for doc in rows:
    #     doc['claims'] = doc['claims'][:1]
    #     try:
    #         wikidata_full.Entry.new_message(**doc).to_bytes_packed()
    #     except:
    #         print(json.dumps(doc['claims'], indent=2))
    #         raise
    message = wikidata_full.Chunk.new_message(entries=rows)
    return message.to_bytes_packed()
    # return message.to_bytes()
    return


def parse_wikidata_json(chunk: bytes):
    chunk = fix_chunk(chunk)
    rows = orjson.loads(chunk)
    rows = [{**row, 'claims': {}} for row in rows]
    rows = delete_hash(rows)
    rows = [demap_recursively(row) for row in rows]
    return orjson.dumps(rows)


def delete_hash(rows: List[dict]):
    result = []
    for doc in rows:
        for claims in doc['claims'].values():
            for claim in claims:
                if 'id' in claim:
                    del claim['id']
                if 'references' in claim:
                    for ref in claim['references']:
                        if 'hash' in ref:
                            del ref['hash']
                if 'qualifiers' in claim:
                    for qualifier in claim['qualifiers'].values():
                        if 'hash' in qualifier:
                            del qualifier['hash']
        result.append(doc)
    return result


def parse_wikidata_simple_msgpack(chunk: bytes):
    return msgpack.packb(delete_hash(parse_wikidata_simple(chunk)), use_bin_type=True)

import msgspec
def parse_wikidata_simple_msgspec(chunk: bytes):
    return msgspec.json.encode(delete_hash(parse_wikidata_simple(chunk)))

# import msgspec
# def parse_wikidata_simple_msgspec(chunk: bytes):
#     return msgspec.msgpack.encode((parse_wikidata_simple(chunk)))


def parse_wikidata_simple_slim(chunk: bytes):
    rows = []
    for doc in parse_wikidata_simple(chunk):
        # rows.append(
        #     {
        #         'id': doc['id']
        #     }
        # )
        # continue
        if 'descriptions' in doc:
            del doc['descriptions']
        if 'labels' in doc:
            del doc['labels']
        if 'aliases' in doc:
            del doc['aliases']
        # del doc['claims']
        for claims in doc['claims'].values():
            for claim in claims:
                if 'id' in claim:
                    del claim['id']
                if 'rank' in claim:
                    del claim['rank']
                if 'references' in claim:
                    for ref in claim['references']:
                        if 'hash' in ref:
                            del ref['hash']
                if 'qualifiers' in claim:
                    for qualifier in claim['qualifiers'].values():
                        if 'hash' in qualifier:
                            del qualifier['hash']

        #     rows.append(doc)
        # return msgpack.packb(rows)
        rows.append(msgpack.packb(doc))
    return rows


def parse_wikidata_simple_turbo(chunk: bytes):
    rows = []
    for doc in parse_wikidata_simple(chunk):
        label = doc['labels'].get('en', {}).get('value') or ''
        description = doc['descriptions'].get('en', {}).get('value') or ''
        aliases = tuple(
            str(e['value'])
            for e in doc['aliases'].get('en', [])
        )
        rows.append(msgpack.packb(dict(
            id=str(doc['id']),
            label=label,
            aliases=aliases,
            description=description,
        )))
    return rows


import capnp

# wikidata = capnp.load('wikidata.capnp')


def parse_wikidata_simple_proto(chunk: bytes):
    rows = []
    for doc in parse_wikidata_simple(chunk):
        label = doc['labels'].get('en', {}).get('value') or ''
        description = doc['descriptions'].get('en', {}).get('value') or ''
        aliases = tuple(
            str(e['value'])
            for e in doc['aliases'].get('en', [])
        )
        data = dict(
            id=str(doc['id']),
            label=label,
            aliases=aliases,
            description=description,
        )
        message = wikidata.Entry.new_message(**data)
        # wikidata.Entry.from_bytes(message.to_bytes())
        # message.to_dict()
        # rows.append(message.to_bytes_packed())
        rows.append(message.to_bytes())
        # rows.append(message)
    # return [wikidata.Chunk.new_message(entries=rows).to_bytes()]

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
        parser=parse_wikidata,
        schema=schema_wikidata,
        file_path_output='wikidata-latest.lance',
    )

    # load_data(
    #     reader=FileChunkReader('.data/wikidata5m_alias.tar.gz:wikidata5m_entity.txt'),
    #     schema=schema_wikidata_5m_entity,
    #     parser=parse_wikidata_5m_entity,
    #     file_path_output='wikidata-5m.lance',
    # )
