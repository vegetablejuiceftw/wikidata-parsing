import capnp
import utils

wikidata_full = capnp.load('../wikidata-full.capnp')

reader = utils.FileChunkReaderGZip(
    '../.data/latest-all.json.gz',  # 27mb/s or 1h:17m
    # '.data/latest-all.json.bz2',  # 6mb/s
    fraction=0.005,
    chunk_size=1024 * 1024 * 8,
)


# rows = []
# for chunk_bytes in reader:
#     chunk = utils.parse_wikidata_simple(chunk_bytes)
#     entry = chunk[0]
#     entry = utils.demap_recursively(entry)
#     # entry['claims'] = []
#     entry['claims'] = list(entry['claims'].values())
#     print(entry)
#     print(type(entry['claims']))
#     wikidata_full.Entry.new_message(**entry)
#     break

def reformat_data_value(data_value: dict):
    if isinstance(data_value['value'], dict):
        data_value['value']['type'] = data_value['type']
        data_value = data_value['value']
    data_value = {k.replace("-", ""): str(v) for k, v in data_value.items()}
    return data_value


def reformat(claim: dict):
    claim['mainsnak']['datavalue'] = reformat_data_value(claim['mainsnak']['datavalue'])
    for snak in claim['qualifiers']:
        snak['datavalue'] = reformat_data_value(snak['datavalue'])
    for reference in claim['references']:
        reference['snaksorder'] = reference.pop('snaks-order')
        for snak in reference['snaks']:
            snak['datavalue'] = reformat_data_value(snak['datavalue'])
    return claim


claim = {
    'mainsnak': {
        'snaktype': 'value',
        'property': 'P2924',
        'datavalue': {
            'value': '4075290',
            'type': 'string'
        },
        'datatype': 'external-id',
    },
    'type': 'statement',
    'id': 'Q31$E182EA43-A631-44AF-94F9-A0FA8DFC3AC2',
    'rank': 'normal',
    'qualifiersorder': [
        'P585'
    ]
}
claim = {
    'mainsnak': {
        'snaktype': 'value',
        'property': 'P1082',
        'datavalue': {
            'value': {
                'amount': '+11150516',
                'unit': '1',
                'numeric-id': '11',
            },
            'type': 'quantity'
        },
        'datatype': 'quantity'
    },
    'type': 'statement',
    'qualifiers': {
        'P585': [
            {
                'snaktype': 'value',
                'property': 'P585',
                'hash': '489481dca5f5660d9fe15875b4dc94a57389e4bd',
                'datavalue': {
                    'value': {
                        'time': '+2014-01-01T00:00:00Z',
                        'timezone': 0,
                        'before': 0,
                        'after': 0,
                        'precision': 11,
                        'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'
                    },
                    'type': 'time'
                },
                'datatype': 'time'
            }
        ]
    },
    'qualifiersorder': [
        'P585'
    ],
    'id': 'Q31$93ba9638-404b-66ac-2733-e6292666a326',
    'rank': 'normal',
    'references': [
        {
            'hash': '9b216970abe8fb8f730ebadbbfecc1b19d17c900',
            'snaks': {
                'P123': [
                    {
                        'snaktype': 'value',
                        'property': 'P123',
                        'datavalue': {
                            'value': {
                                'entity-type': 'item',
                                'numeric-id': 12480,
                                'id': 'Q12480'
                            },
                            'type': 'wikibase-entityid'
                        },
                        'datatype': 'wikibase-item'
                    }
                ],
                'P585': [
                    {
                        'snaktype': 'value',
                        'property': 'P585',
                        'datavalue': {
                            'value': {
                                'time': '+2014-01-01T00:00:00Z',
                                'timezone': 0,
                                'before': 0,
                                'after': 0,
                                'precision': 11,
                                'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'
                            },
                            'type': 'time'
                        },
                        'datatype': 'time'
                    }
                ]
            },
            'snaks-order': [
                'P123',
                'P585'
            ]
        }
    ]
}
claim = utils.demap_recursively(claim)
claim = reformat(claim)
print(claim)
print(claim['references'])
claim = wikidata_full.Claim.new_message(**claim)
claim = wikidata_full.Claim.from_bytes_packed(claim.to_bytes_packed())
print(claim)
print(claim.to_dict()['mainsnak'])

# for i in range(20):
#     print(i)

# 0
# 1
# 2
# 3
# 4
# 5
# 6
# 7
# 8
# 9
# 10
# 11
# 12
# 13
# 14
# 15
# 16
# 17
# 18
# 19
