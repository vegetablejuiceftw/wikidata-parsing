import lance

import pandas as pd
import pyarrow as pa
import pyarrow.dataset

dataset = lance.dataset("wikidata.lance")

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

chunk_size = 10
for i in tqdm(range(0, dataset.count_rows(), chunk_size)):
    # a = dataset.take(list(range(i, i + chunk_size)))
    a = dataset.take(sample(list(range(dataset.count_rows())), chunk_size))

print(a[:2])
