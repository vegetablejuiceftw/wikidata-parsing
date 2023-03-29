import lance

import pandas as pd
import pyarrow as pa
import pyarrow.dataset
from pyarrow import RecordBatch

# dataset = lance.dataset("wikidata-latest.lance-beta")
dataset = lance.dataset("vec_data.lance")
# dataset = lance.dataset("wikidata-5m.lance")

# print(len(dataset))
print(dataset.count_rows())
# print(dataset.head(1))
# # print(dataset.head(1)['label'])
# print(dataset.take([38_000]))

# for e in dataset.head(10):
#     print(e)

# dataset.to_table().to_pandas()


from tqdm.auto import tqdm

from random import sample

# chunk_size = 100
# for i in tqdm(range(0, dataset.count_rows(), chunk_size)):
#     a = dataset.take(list(range(i, i + chunk_size)))
    # a = dataset.take(sample(list(range(dataset.count_rows())), chunk_size))
#
# print(a[:2])

for batch in dataset.to_table().to_batches():
    batch: RecordBatch
    # print(batch)
    # print(repr(batch))
    print(batch.to_pylist()[0])
    print(batch['label'])
    break


# max_chunksize
def run():
    for item in dataset.to_table(columns=['label']).to_batches():
        # for row in item['label']:
        #     yield row
        for row in item.to_pylist():
            yield row
        # for row in item.to_pandas().to_records():
        #     yield row

for row in tqdm(run(), total=dataset.count_rows()):
    pass
