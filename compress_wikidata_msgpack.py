"""
Compress a jsonl version of Wikidata by throwing about descriptions
and converting file to msgpack format.

Usage
-----

```
python3 compress_wikidata_msgpack.py /home/fred/PycharmProjects/pythonProject/.data/latest-all.json.bz2 wikidata.mp
python3 compress_wikidata_msgpack.py wikidata.mp wikidata2.mp
```

"""
import argparse
import msgpack
import json
from tqdm.auto import tqdm
from wikidata_linker_utils.wikidata_iterator import open_wikidata_file

import capnp
wikidata = capnp.load('wikidata.capnp')

def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('wikidata')
    parser.add_argument('out')
    return parser.parse_args(args=args)

import gzip


def main():
    args = parse_args()
    packer = msgpack.Packer()
    # with gzip.open(args.out, 'wb') as fout:
    with open(args.out, "wb") as fout:
        for doc in tqdm(open_wikidata_file(args.wikidata, 1000), mininterval=1, position=1):
            # wikidata.Entry.from_bytes(doc)
            # msgpack.unpackb(doc)
            # fout.write(packer.pack(msgpack.unpackb(doc)))
            # fout.write(msgpack.packb(doc))
            # fout.write(doc)
            # print(type(doc), len(doc))
            # print(type(dict(wikidata.Entry.from_bytes_packed(doc))))
            # fout.write(json.dumps(wikidata.Entry.from_bytes(doc).to_dict())+"\n")
            # wikidata.Entry.from_bytes(doc).write(fout)
            continue
            if 'descriptions' in doc:
                del doc['descriptions']
            if 'labels' in doc:
                del doc['labels']
            if 'aliases' in doc:
                del doc['aliases']
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
            fout.write(msgpack.packb(doc))


#   30000it [01:25,   351.53it/s] : basic
#   26454it [00:59,   444.78it/s] : msgpack file
#   24906it [00:13,  1839.97it/s] : fred bz2 MP
#   84658it [00:27,  3108.43it/s] : fred SP
# 1056933it [00:56, 30179.07it/s] : fred GZ MP

# Processing chunks: 2.46GB [01:05, 37.7MB/s] processing .gz
# 2183733it [00:18, 118934.96it/s] messagepack.mp 180 MB
# 2183733it [00:03, 714627.62it/s] capnp.p [packed] 160 MB
# 2183733it [00:02, 766504.72it/s] capnp.p 255 MB
# 2183733it [00:02, 755414.57it/s] capnp.p 50mb os.popen(f"cat {path} | gzip -d")

if __name__ == "__main__":
    main()
