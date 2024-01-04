import gzip
import os
from collections import Counter
from itertools import islice
from multiprocessing import Pool
import glob

import msgpack
from tqdm.auto import tqdm

from constants import DISABLED_PROPS, DISABLED_ALWAYS, DISABLED_UNLINKED
from utils import get_labels


def show(counter: Counter, count=32):
    total = sum(counter.values())

    common = counter.most_common(count)
    c = 0
    for i in range(0, len(common), 48):
        ids, counts = zip(*common[i:i + 48])
        ids = [idx.decode("utf-8") for idx in ids]
        labels = get_labels(ids)
        for k, v in zip(ids, counts):
            c += 1
            print(f"{c: <3} {v: <8} {v / total * 100:.2f}  {k} {labels.get(k)!r}")


def process(line: dict):
    qid = line[b'id']
    sitelinks = line[b'sitelinks']

    props = tuple(p.get(b'property') for p in line[b'claims'])
    props = {p for p in props if p not in DISABLED_PROPS}

    if not sitelinks and DISABLED_UNLINKED & props:
        return None

    instance_of = []
    subclass_of = []
    for p in line[b'claims']:
        idx = p.get(b'id')
        if p.get(b'property') == b'P31':
            instance_of.append(idx)
        if p.get(b'property') == b'P279':
            subclass_of.append(idx)

    if any(instance in DISABLED_ALWAYS for instance in instance_of):
        return None

    if not sitelinks and any(instance in DISABLED_UNLINKED for instance in instance_of):
        return None

    return {
        "claims": props,
        "instance_of": instance_of,
        "subclass_of": subclass_of,
    }


def reader(file_path):
    file_size = os.path.getsize(file_path)

    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=file_path, mininterval=1.5, position=0)

    counter_props = Counter()
    counter_instance_of = Counter()
    counter_subclass_of = Counter()
    counter_filtering = Counter()

    old = 0
    with gzip.open(file_path, 'rb') as file:
        try:
            for line in islice(msgpack.Unpacker(file, use_list=False, raw=True), None):
                new_start_pos = file.fileobj.tell()
                pbar.update(new_start_pos - old)
                old = new_start_pos

                line = process(line)
                if line:
                    counter_props.update(line['claims'])
                    counter_instance_of.update(line['instance_of'])
                    counter_subclass_of.update(line['subclass_of'])
                    counter_filtering.update(["allowed"])
                    if not line['claims']:
                        counter_filtering.update(["allowed-no-claims"])
                    if not line['instance_of']:
                        counter_filtering.update(["allowed-no-instance-of"])
                else:
                    counter_filtering.update(["discarded"])
        except:
            pass
    return counter_filtering, counter_props, counter_instance_of, counter_subclass_of


counter_filtering = Counter()
counter_props = Counter()
counter_instance_of = Counter()
counter_subclass_of = Counter()

SHARD_PATH = "../data/shard-16-*.gz"
shards = list(glob.glob(SHARD_PATH))
assert shards, f"are we sure that the `SHARD_PATH = {SHARD_PATH}` is correct?"

with Pool(len(shards)) as p:
    for f, p, i, s in p.imap(reader, shards):
        counter_filtering.update(f)
        counter_props.update(p)
        counter_instance_of.update(i)
        counter_subclass_of.update(s)

print("\nFiltering\n", *counter_filtering.most_common(), sep="\n")

print("\ncounter_props")
show(counter_props, count=128)

print("\ncounter_instance_of")
show(counter_instance_of, 128)
#
print("\ncounter_subclass_of")
show(counter_subclass_of, 128)
