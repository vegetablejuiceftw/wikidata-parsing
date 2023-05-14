##
import os

os.chdir('../')
print(os.getcwd())

import msgpack
from tqdm.auto import tqdm
import dbm
import utils
import msgspec
import orjson


reader = utils.FileChunkReaderGZip(
    '.data/latest-all.json.gz',  # 27mb/s or 1h:17m
    # '.data/latest-all.json.bz2',  # 6mb/s
    fraction=0.005,
    chunk_size=1024 * 1024 * 8,
)

def reformat_data_value(data_value: dict):
    if not data_value:
        return data_value
    if isinstance(data_value['value'], dict):
        data_value['value']['type'] = data_value['type']
        data_value = data_value['value']
    data_value = {k.replace("-", ""): str(v) for k, v in data_value.items()}
    return data_value


def reformat(claim: dict):
    # print()
    # print(claim)
    claim['mainsnak']['datavalue'] = reformat_data_value(claim['mainsnak'].get('datavalue', {}))
    claim['qualifiersorder'] = claim.pop('qualifiers-order', [])

    for snak in claim.get('qualifiers', []):
        # print(snak)
        snak['datavalue'] = reformat_data_value(snak.get('datavalue', {}))
    for reference in claim.get('references', []):
        reference['snaksorder'] = reference.pop('snaks-order')
        for snak in reference['snaks']:
            # print(snak)

            snak['datavalue'] = reformat_data_value(snak.get('datavalue', {}))
    return claim


import capnp

wikidata_full = capnp.load('wikidata-full.capnp')

rows = []
for chunk_bytes in reader:
    chunk_raw = utils.parse_wikidata_capnp(chunk_bytes)
    chunk_raw = utils.parse_wikidata_simple(chunk_bytes)

    for doc in chunk_raw:
        # print(doc)
        # print(str(doc['claims'])[:256])

        doc = utils.demap_recursively(doc)
        doc['claims'] = doc['claims'] or {}
        # print(doc['claims'])
        doc['claims'] = [reformat(claim) for claim in sum(doc['claims'].values(), start=[])]

        chunk_raw = wikidata_full.Entry.new_message(**doc).to_bytes_packed()
        # print(type(chunk_raw), len(chunk_raw))
    break

##
# processes
# 0 - 106s
# 2 - 56s
# 4 - 31s
# 8 - 19s
# 16 - 16s
# 32 - 16s
# for chunk_raw in reader:
#     pass
#     chunk_raw = utils.parse_wikidata(chunk_raw)

for chunk_raw in utils.run_function(reader, utils.parse_wikidata_capnp):
    # chunk: list = msgpack.unpackb(chunk_raw, raw=False)
    # chunk: list = msgspec.json.decode(chunk_raw)
    # chunk: list = orjson.loads(chunk_raw)
    chunk = utils.wikidata_full.Chunk.from_bytes_packed(chunk_raw)
    rows.extend([e.title for e in chunk.entries])
    # chunk.to_dict()
    # rows.extend(chunk.entries)
    # with utils.wikidata_full.Chunk.from_bytes(chunk_raw) as chunk:
    #     rows.extend(chunk.entries)
    # print(len(chunk.entries))
    pass
    # break
exit()
##
print(len(rows), rows[0].keys())
# print(chunk[0])
##

example = rows[0]
# print(example['sitelinks'])
import json

print(json.dumps(utils.demap_recursively(example['claims']), indent=2))

# for row in tqdm(rows):
#     if not isinstance(demap_recursively(row)['aliases'], list):
#         # print(row['aliases'].keys())
#         print(list(row['aliases'].values())[0])
#         print(set(row['aliases'].keys()) - set(e['language'] for e in row['aliases'].values()))

##
from genson import SchemaBuilder

builder = SchemaBuilder()
builder.add_schema({"type": "object", "properties": {
    'claims': {
        "type": "object",
        "patternProperties": {
            r'^.*$': None,
        },
    }
}})
print("SSS")

for row in tqdm(rows[:10_00]):
    # del row['claims']
    # row = dict(row)
    # row['claims'] = list(row['claims'].values())
    row = utils.demap_recursively(row)
    builder.add_object(row)

print(builder.to_json(indent=2))
print(builder.to_json(indent=2).count("\n"))
# 883801
#  45804
#    477

##
set(row['type'] for row in rows)

##
schema = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "claims": {
            "anyOf": [
                {
                    "type": "array"
                },
                {
                    "type": "object",
                    "patternProperties": {
                        "^.*$": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "mainsnak": {
                                        "type": "object",
                                        "properties": {
                                            "snaktype": {
                                                "type": "string"
                                            },
                                            "property": {
                                                "type": "string"
                                            },
                                            "datavalue": {
                                                "type": "object",
                                                "properties": {
                                                    "value": {
                                                        "anyOf": [
                                                            {
                                                                "type": "string"
                                                            },
                                                            {
                                                                "type": "object",
                                                                "properties": {
                                                                    "entity-type": {
                                                                        "type": "string"
                                                                    },
                                                                    "numeric-id": {
                                                                        "type": "integer"
                                                                    },
                                                                    "id": {
                                                                        "type": "string"
                                                                    },
                                                                    "amount": {
                                                                        "type": "string"
                                                                    },
                                                                    "unit": {
                                                                        "type": "string"
                                                                    },
                                                                    "latitude": {
                                                                        "type": "number"
                                                                    },
                                                                    "longitude": {
                                                                        "type": "number"
                                                                    },
                                                                    "altitude": {
                                                                        "type": "null"
                                                                    },
                                                                    "precision": {
                                                                        "type": [
                                                                            "null",
                                                                            "number"
                                                                        ]
                                                                    },
                                                                    "globe": {
                                                                        "type": "string"
                                                                    },
                                                                    "text": {
                                                                        "type": "string"
                                                                    },
                                                                    "language": {
                                                                        "type": "string"
                                                                    },
                                                                    "time": {
                                                                        "type": "string"
                                                                    },
                                                                    "timezone": {
                                                                        "type": "integer"
                                                                    },
                                                                    "before": {
                                                                        "type": "integer"
                                                                    },
                                                                    "after": {
                                                                        "type": "integer"
                                                                    },
                                                                    "calendarmodel": {
                                                                        "type": "string"
                                                                    },
                                                                    "upperBound": {
                                                                        "type": "string"
                                                                    },
                                                                    "lowerBound": {
                                                                        "type": "string"
                                                                    }
                                                                }
                                                            }
                                                        ]
                                                    },
                                                    "type": {
                                                        "type": "string"
                                                    }
                                                },
                                                "required": [
                                                    "type",
                                                    "value"
                                                ]
                                            },
                                            "datatype": {
                                                "type": "string"
                                            }
                                        },
                                        "required": [
                                            "datatype",
                                            "property",
                                            "snaktype"
                                        ]
                                    },
                                    "type": {
                                        "type": "string"
                                    },
                                    "id": {
                                        "type": "string"
                                    },
                                    "rank": {
                                        "type": "string"
                                    },
                                    "qualifiers": {
                                        "type": "array",
                                        "items": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "snaktype": {
                                                        "type": "string"
                                                    },
                                                    "property": {
                                                        "type": "string"
                                                    },
                                                    "hash": {
                                                        "type": "string"
                                                    },
                                                    "datavalue": {
                                                        "type": "object",
                                                        "properties": {
                                                            "value": {
                                                                "anyOf": [
                                                                    {
                                                                        "type": "string"
                                                                    },
                                                                    {
                                                                        "type": "object",
                                                                        "properties": {
                                                                            "time": {
                                                                                "type": "string"
                                                                            },
                                                                            "timezone": {
                                                                                "type": "integer"
                                                                            },
                                                                            "before": {
                                                                                "type": "integer"
                                                                            },
                                                                            "after": {
                                                                                "type": "integer"
                                                                            },
                                                                            "precision": {
                                                                                "type": "number"
                                                                            },
                                                                            "calendarmodel": {
                                                                                "type": "string"
                                                                            },
                                                                            "entity-type": {
                                                                                "type": "string"
                                                                            },
                                                                            "numeric-id": {
                                                                                "type": "integer"
                                                                            },
                                                                            "id": {
                                                                                "type": "string"
                                                                            },
                                                                            "amount": {
                                                                                "type": "string"
                                                                            },
                                                                            "unit": {
                                                                                "type": "string"
                                                                            },
                                                                            "text": {
                                                                                "type": "string"
                                                                            },
                                                                            "language": {
                                                                                "type": "string"
                                                                            },
                                                                            "latitude": {
                                                                                "type": "number"
                                                                            },
                                                                            "longitude": {
                                                                                "type": "number"
                                                                            },
                                                                            "altitude": {
                                                                                "type": "null"
                                                                            },
                                                                            "globe": {
                                                                                "type": "string"
                                                                            },
                                                                            "upperBound": {
                                                                                "type": "string"
                                                                            },
                                                                            "lowerBound": {
                                                                                "type": "string"
                                                                            }
                                                                        }
                                                                    }
                                                                ]
                                                            },
                                                            "type": {
                                                                "type": "string"
                                                            }
                                                        },
                                                        "required": [
                                                            "type",
                                                            "value"
                                                        ]
                                                    },
                                                    "datatype": {
                                                        "type": "string"
                                                    }
                                                },
                                                "required": [
                                                    "datatype",
                                                    "hash",
                                                    "property",
                                                    "snaktype"
                                                ]
                                            }
                                        }
                                    },
                                    "qualifiers-order": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    "references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "hash": {
                                                    "type": "string"
                                                },
                                                "snaks": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "snaktype": {
                                                                    "type": "string"
                                                                },
                                                                "property": {
                                                                    "type": "string"
                                                                },
                                                                "datavalue": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "value": {
                                                                            "anyOf": [
                                                                                {
                                                                                    "type": "string"
                                                                                },
                                                                                {
                                                                                    "type": "object",
                                                                                    "properties": {
                                                                                        "entity-type": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "numeric-id": {
                                                                                            "type": "integer"
                                                                                        },
                                                                                        "id": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "time": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "timezone": {
                                                                                            "type": "integer"
                                                                                        },
                                                                                        "before": {
                                                                                            "type": "integer"
                                                                                        },
                                                                                        "after": {
                                                                                            "type": "integer"
                                                                                        },
                                                                                        "precision": {
                                                                                            "type": "number"
                                                                                        },
                                                                                        "calendarmodel": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "text": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "language": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "amount": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "unit": {
                                                                                            "type": "string"
                                                                                        },
                                                                                        "latitude": {
                                                                                            "type": "number"
                                                                                        },
                                                                                        "longitude": {
                                                                                            "type": "number"
                                                                                        },
                                                                                        "altitude": {
                                                                                            "type": "null"
                                                                                        },
                                                                                        "globe": {
                                                                                            "type": "string"
                                                                                        }
                                                                                    }
                                                                                }
                                                                            ]
                                                                        },
                                                                        "type": {
                                                                            "type": "string"
                                                                        }
                                                                    },
                                                                    "required": [
                                                                        "type",
                                                                        "value"
                                                                    ]
                                                                },
                                                                "datatype": {
                                                                    "type": "string"
                                                                }
                                                            },
                                                            "required": [
                                                                "datatype",
                                                                "property",
                                                                "snaktype"
                                                            ]
                                                        }
                                                    }
                                                },
                                                "snaks-order": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "string"
                                                    }
                                                }
                                            },
                                            "required": [
                                                "hash",
                                                "snaks",
                                                "snaks-order"
                                            ]
                                        }
                                    }
                                },
                                "required": [
                                    "id",
                                    "mainsnak",
                                    "rank",
                                    "type"
                                ]
                            }
                        }
                    }
                }
            ]
        },
        "type": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "labels": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                },
                "required": [
                    "language",
                    "value"
                ]
            }
        },
        "descriptions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                },
                "required": [
                    "language",
                    "value"
                ]
            }
        },
        "aliases": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "language": {
                            "type": "string"
                        },
                        "value": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "language",
                        "value"
                    ]
                }
            }
        },
        "sitelinks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string"
                    },
                    "badges": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": [
                    "badges",
                    "site",
                    "title"
                ]
            }
        },
        "pageid": {
            "type": "integer"
        },
        "ns": {
            "type": "integer"
        },
        "title": {
            "type": "string"
        },
        "lastrevid": {
            "type": "integer"
        },
        "modified": {
            "type": "string"
        }
    },
    "required": [
        "aliases",
        "claims",
        "descriptions",
        "id",
        "labels",
        "lastrevid",
        "modified",
        "ns",
        "pageid",
        "sitelinks",
        "title",
        "type"
    ]
}
498

old = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "claims": {
            "type": "object",
            "patternProperties": {
                "^.*$": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "mainsnak": {
                                "type": "object",
                                "properties": {
                                    "snaktype": {
                                        "type": "string"
                                    },
                                    "property": {
                                        "type": "string"
                                    },
                                    "datavalue": {
                                        "type": "object",
                                        "properties": {
                                            "value": {
                                                "anyOf": [
                                                    {
                                                        "type": "string"
                                                    },
                                                    {
                                                        "type": "object",
                                                        "properties": {
                                                            "entity-type": {
                                                                "type": "string"
                                                            },
                                                            "numeric-id": {
                                                                "type": "integer"
                                                            },
                                                            "id": {
                                                                "type": "string"
                                                            },
                                                            "amount": {
                                                                "type": "string"
                                                            },
                                                            "unit": {
                                                                "type": "string"
                                                            },
                                                            "latitude": {
                                                                "type": "number"
                                                            },
                                                            "longitude": {
                                                                "type": "number"
                                                            },
                                                            "altitude": {
                                                                "type": "null"
                                                            },
                                                            "precision": {
                                                                "type": [
                                                                    "null",
                                                                    "number"
                                                                ]
                                                            },
                                                            "globe": {
                                                                "type": "string"
                                                            },
                                                            "text": {
                                                                "type": "string"
                                                            },
                                                            "language": {
                                                                "type": "string"
                                                            },
                                                            "time": {
                                                                "type": "string"
                                                            },
                                                            "timezone": {
                                                                "type": "integer"
                                                            },
                                                            "before": {
                                                                "type": "integer"
                                                            },
                                                            "after": {
                                                                "type": "integer"
                                                            },
                                                            "calendarmodel": {
                                                                "type": "string"
                                                            },
                                                            "upperBound": {
                                                                "type": "string"
                                                            },
                                                            "lowerBound": {
                                                                "type": "string"
                                                            }
                                                        }
                                                    }
                                                ]
                                            },
                                            "type": {
                                                "type": "string"
                                            }
                                        },
                                        "required": [
                                            "type",
                                            "value"
                                        ]
                                    },
                                    "datatype": {
                                        "type": "string"
                                    }
                                },
                                "required": [
                                    "datatype",
                                    "property",
                                    "snaktype"
                                ]
                            },
                            "type": {
                                "type": "string"
                            },
                            "id": {
                                "type": "string"
                            },
                            "rank": {
                                "type": "string"
                            },
                            "qualifiers": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "snaktype": {
                                                "type": "string"
                                            },
                                            "property": {
                                                "type": "string"
                                            },
                                            "hash": {
                                                "type": "string"
                                            },
                                            "datavalue": {
                                                "type": "object",
                                                "properties": {
                                                    "value": {
                                                        "anyOf": [
                                                            {
                                                                "type": "string"
                                                            },
                                                            {
                                                                "type": "object",
                                                                "properties": {
                                                                    "time": {
                                                                        "type": "string"
                                                                    },
                                                                    "timezone": {
                                                                        "type": "integer"
                                                                    },
                                                                    "before": {
                                                                        "type": "integer"
                                                                    },
                                                                    "after": {
                                                                        "type": "integer"
                                                                    },
                                                                    "precision": {
                                                                        "type": "number"
                                                                    },
                                                                    "calendarmodel": {
                                                                        "type": "string"
                                                                    },
                                                                    "entity-type": {
                                                                        "type": "string"
                                                                    },
                                                                    "numeric-id": {
                                                                        "type": "integer"
                                                                    },
                                                                    "id": {
                                                                        "type": "string"
                                                                    },
                                                                    "amount": {
                                                                        "type": "string"
                                                                    },
                                                                    "unit": {
                                                                        "type": "string"
                                                                    },
                                                                    "text": {
                                                                        "type": "string"
                                                                    },
                                                                    "language": {
                                                                        "type": "string"
                                                                    },
                                                                    "latitude": {
                                                                        "type": "number"
                                                                    },
                                                                    "longitude": {
                                                                        "type": "number"
                                                                    },
                                                                    "altitude": {
                                                                        "type": "null"
                                                                    },
                                                                    "globe": {
                                                                        "type": "string"
                                                                    },
                                                                    "upperBound": {
                                                                        "type": "string"
                                                                    },
                                                                    "lowerBound": {
                                                                        "type": "string"
                                                                    }
                                                                }
                                                            }
                                                        ]
                                                    },
                                                    "type": {
                                                        "type": "string"
                                                    }
                                                },
                                                "required": [
                                                    "type",
                                                    "value"
                                                ]
                                            },
                                            "datatype": {
                                                "type": "string"
                                            }
                                        },
                                        "required": [
                                            "datatype",
                                            "hash",
                                            "property",
                                            "snaktype"
                                        ]
                                    }
                                }
                            },
                            "qualifiers-order": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            },
                            "references": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "hash": {
                                            "type": "string"
                                        },
                                        "snaks": {
                                            "type": "array",
                                            "items": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "snaktype": {
                                                            "type": "string"
                                                        },
                                                        "property": {
                                                            "type": "string"
                                                        },
                                                        "datavalue": {
                                                            "type": "object",
                                                            "properties": {
                                                                "value": {
                                                                    "anyOf": [
                                                                        {
                                                                            "type": "string"
                                                                        },
                                                                        {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "entity-type": {
                                                                                    "type": "string"
                                                                                },
                                                                                "numeric-id": {
                                                                                    "type": "integer"
                                                                                },
                                                                                "id": {
                                                                                    "type": "string"
                                                                                },
                                                                                "time": {
                                                                                    "type": "string"
                                                                                },
                                                                                "timezone": {
                                                                                    "type": "integer"
                                                                                },
                                                                                "before": {
                                                                                    "type": "integer"
                                                                                },
                                                                                "after": {
                                                                                    "type": "integer"
                                                                                },
                                                                                "precision": {
                                                                                    "type": "integer"
                                                                                },
                                                                                "calendarmodel": {
                                                                                    "type": "string"
                                                                                },
                                                                                "text": {
                                                                                    "type": "string"
                                                                                },
                                                                                "language": {
                                                                                    "type": "string"
                                                                                },
                                                                                "amount": {
                                                                                    "type": "string"
                                                                                },
                                                                                "unit": {
                                                                                    "type": "string"
                                                                                }
                                                                            }
                                                                        }
                                                                    ]
                                                                },
                                                                "type": {
                                                                    "type": "string"
                                                                }
                                                            },
                                                            "required": [
                                                                "type",
                                                                "value"
                                                            ]
                                                        },
                                                        "datatype": {
                                                            "type": "string"
                                                        }
                                                    },
                                                    "required": [
                                                        "datatype",
                                                        "property",
                                                        "snaktype"
                                                    ]
                                                }
                                            }
                                        },
                                        "snaks-order": {
                                            "type": "array",
                                            "items": {
                                                "type": "string"
                                            }
                                        }
                                    },
                                    "required": [
                                        "hash",
                                        "snaks",
                                        "snaks-order"
                                    ]
                                }
                            }
                        },
                        "required": [
                            "id",
                            "mainsnak",
                            "rank",
                            "type"
                        ]
                    }
                }
            }
        },
        "type": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "labels": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                },
                "required": [
                    "language",
                    "value"
                ]
            }
        },
        "descriptions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                },
                "required": [
                    "language",
                    "value"
                ]
            }
        },
        "aliases": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "language": {
                            "type": "string"
                        },
                        "value": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "language",
                        "value"
                    ]
                }
            }
        },
        "sitelinks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string"
                    },
                    "badges": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": [
                    "badges",
                    "site",
                    "title"
                ]
            }
        },
        "pageid": {
            "type": "integer"
        },
        "ns": {
            "type": "integer"
        },
        "title": {
            "type": "string"
        },
        "lastrevid": {
            "type": "integer"
        },
        "modified": {
            "type": "string"
        }
    },
    "required": [
        "aliases",
        "claims",
        "descriptions",
        "id",
        "labels",
        "lastrevid",
        "modified",
        "ns",
        "pageid",
        "sitelinks",
        "title",
        "type"
    ]
}
