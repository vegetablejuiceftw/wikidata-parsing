import json
import msgpack
import bz2

import utils

def iterate_bytes_jsons(fin, batch_size=1000):
    current = []
    for l in fin:
        if l.startswith(b'{'):
            current.append(l)
        if len(current) >= batch_size:
            docs = json.loads('[' + b"".join(current).decode('utf-8').rstrip(',\n') + ']')
            for doc in docs:
                yield doc
            current = []
    if len(current) > 0:
        docs = json.loads('[' + b"".join(current).decode('utf-8').rstrip(',\n') + ']')
        for doc in docs:
            yield doc
        current = []


def iterate_text_jsons(fin, batch_size=1000):
    current = []
    for l in fin:
        if l.startswith('{'):
            current.append(l)
        if len(current) >= batch_size:
            docs = json.loads('[' + "".join(current).rstrip(',\n') + ']')
            for doc in docs:
                yield doc
            current = []
    if len(current) > 0:
        docs = json.loads('[' + "".join(current).rstrip(',\n') + ']')
        for doc in docs:
            yield doc
        current = []


def iterate_message_packs(fin):

    unpacker = msgpack.Unpacker(fin, encoding='utf-8', use_list=False)
    for obj in unpacker:
        yield obj


import capnp
wikidata = capnp.load('wikidata.capnp')
def iterate_proto_packs_packed(fin):
    for obj in wikidata.Entry.read_multiple_packed(fin):
        yield obj
def iterate_proto_packs(fin):
    for obj in wikidata.Entry.read_multiple(fin):
        yield obj

import gzip
import os


class LenGzipFile(gzip.GzipFile):
    def __len__(self):
        self.seek(0, os.SEEK_END)
        file_size = self.tell()
        self.seek(0)
        return file_size

def open_wikidata_file(path, batch_size):
    if path.endswith('bz2'):
        with bz2.open(path, 'rb') as fin:
            for obj in iterate_bytes_jsons(fin, batch_size):
                yield obj
    elif path.endswith('json'):
        with open(path, 'rt') as fin:
            for obj in iterate_text_jsons(fin, batch_size):
                yield obj
    elif path.endswith('mp'):
        with open(path, 'rb') as fin:
            for obj in iterate_message_packs(fin):
                yield obj
    elif path.endswith('.p'):
        with open(path, 'rb') as fin:
            for obj in iterate_proto_packs_packed(fin):
                yield obj
    elif path.endswith('.pp'):
        with open(path, 'rb') as fin:
            for obj in iterate_proto_packs(fin):
                yield obj
    elif path.endswith('.pp.gz'):
        with os.popen(f"cat {path} | gzip -d") as fin:
            print(type(fin))

        # with LenGzipFile(path, 'rb') as fin:
        #     def foo():
        #         # while 1:
        #         yield fin.read(1024 * 1023)

            # for obj in wikidata.Entry.read_multiple_bytes(list(foo())):
            # print(len(fin))
            for obj in iterate_proto_packs(fin):
                yield obj

    elif path.endswith('gz'):
        reader = utils.FileChunkReaderGZip(
            '.data/latest-all.json.gz',  # 27mb/s or 1h:17m
            # '.data/latest-all.json.bz2',  # 6mb/s
            fraction=0.02,
            # fraction=0.002,
            chunk_size= 1024 * 1024 * 8,
        )
        for chunk in utils.run_function(reader, utils.parse_wikidata_simple_proto):
            for obj in chunk:
                yield obj
    else:
        raise ValueError(
            "unknown extension for wikidata. "
            "Expecting bz2, json, or mp (msgpack)."
        )

