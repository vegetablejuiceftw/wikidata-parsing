import multiprocessing, gzip, os

import orjson
from tqdm.auto import tqdm

file_path = '/home/fred/Downloads/wikidata-20220103-all.json.gz'
chunk_size = 1024 * 1024 * 8


def read_bytes(filename, start_pos, end_pos):
    # print(start_pos, end_pos)
    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc='Processing Wikidata dump')

    with open(filename, 'rb') as file, gzip.open(file, 'rb') as f_gz:
        _garbage = f_gz.readline()  # drop the initial b"[\n"
        # print(file.tell(), f.tell(), _garbage)

        buffer = b''
        while start_pos < end_pos:
            result = f_gz.read(chunk_size)
            new_start_pos = f_gz.tell()
            change = new_start_pos - start_pos
            pbar.update(change)
            start_pos = new_start_pos

            head, *tail = result.rsplit(b'\n', 1)
            tail = tail[0] if tail else b''
            head = buffer + head
            buffer = tail

            head = b"[" + head.strip(b',') + b"]"
            yield head


def parse(chunk: bytes):
    data = []
    for row in orjson.loads(chunk):
        label = row['labels'].get('en', {}).get('value', '')
        if "Pew Charitable Trusts".lower() in label.lower():
            print(row)
        if "Q201296".lower() in row['id'].lower():
            print(row)
        if not label:
            continue
        if not row['type'] == 'item':
            continue

        sitelink = row['sitelinks'].get('enwiki', {})
        sitelink, sitebadges = sitelink.get('title'), tuple(sitelink.get('badges', tuple()))
        sitelink_count = len(row['sitelinks'])
        label_count = len(row['labels'])
        description = row['descriptions'].get('en', {}).get('value')
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


if __name__ == '__main__':
    filename = file_path
    file_size = os.path.getsize(filename)
    file_size = file_size // 1
    print("file_size", file_size)

    # for c in read_bytes(filename, 0, file_size):
    #     pass

    num_processes = min(6, multiprocessing.cpu_count())  # Use available CPU cores
    result = []
    with multiprocessing.Pool(num_processes) as pool:
        for r in pool.imap_unordered(parse, read_bytes(filename, 0, file_size)):
            result.extend(r)

    print(result[0])
    import lance
    import pandas as pd

    df = pd.DataFrame(result)
    with pd.option_context('display.max_columns', None, 'max_colwidth', -1, 'display.width', 1000):
        print(df)

    dataset = lance.write_dataset(df, "wikidata2.lance")
    #           label    sitelink
    # 38057  surprise  Surprise (emotion)
