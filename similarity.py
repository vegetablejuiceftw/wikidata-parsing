##
import lance

dataset = lance.dataset("wikidata-5m.lance")
# dataset = lance.dataset("wikidata-latest-86m.lance")

##
import re
from ftfy import fix_text
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm.auto import tqdm


def ngrams(string, n=3):
    string = fix_text(string)  # fix text
    string = string.encode("ascii", errors="ignore").decode()  # remove non ascii chars
    string = string.lower()
    string = re.sub(r"[)(.|\[\]{}']", '', string)
    string = re.sub(r'[,-./]\s*', ' ', string)
    string = string.replace('&', 'and')
    return tuple(
        string[i:i + n]
        for i in range(0, max(len(string) - 2, 1))
    )


print('All 3-grams in "Department":')
print(ngrams('Department Pants'))
print(ngrams('OK'))


##
def run(*columns):
    for item in dataset.to_table(columns=columns or None).to_batches():
        for row in item.to_pylist():
            yield row


org_names = tuple(row['label'] for row in run('label'))
# from utils import run_function
# run_function(tqdm(org_names, total=dataset.count_rows()), ngrams)

##
from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer

print("start")
vectorizer = HashingVectorizer(analyzer=ngrams, n_features=2 ** 13)
tf_idf_matrix = vectorizer.fit_transform(tqdm(org_names, total=dataset.count_rows()))
print("done")

##
print("start")
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams, dtype=np.float32)
vectorizer.fit(tqdm(org_names, total=dataset.count_rows()))
print("done")

##
print("start")
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix = vectorizer.fit_transform(tqdm(org_names, total=dataset.count_rows()))
print("done")

##
import sparse_dot_topn.sparse_dot_topn as ct
from scipy.sparse import csr_matrix


def awesome_cossim_top(A, B, ntop, lower_bound=0):
    # force A and B as a CSR matrix.
    # If they have already been CSR, there is no overhead
    A = A.tocsr()
    B = B.tocsr()
    M, _ = A.shape
    _, N = B.shape

    idx_dtype = np.int32

    nnz_max = M * ntop

    indptr = np.zeros(M + 1, dtype=idx_dtype)
    indices = np.zeros(nnz_max, dtype=idx_dtype)
    data = np.zeros(nnz_max, dtype=A.dtype)

    ct.sparse_dot_topn(
        M, N, np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        lower_bound,
        indptr, indices, data)

    return csr_matrix((data, indices, indptr), shape=(M, N))


import time

t1 = time.time()
matches = awesome_cossim_top(tf_idf_matrix[:100], tf_idf_matrix[100:].transpose(), 10, 0.85)
t = time.time() - t1
print("SELFTIMED:", t)
# print(matches)

non_zeros = matches.nonzero()

sparserows = non_zeros[0]
sparsecols = non_zeros[1]

for r, c in zip(sparserows, sparsecols):
    print(r, c)
    print(org_names[r])
    print(org_names[c + 100])

##
# with pd.option_context('display.max_columns', None, 'max_colwidth', -1, 'display.width', 1000):
#     print(df.loc[[2, 4017343+100]])
org_names = list(row['label'] for row in run('label'))
##
import spacy

nlp = spacy.load("en_core_web_lg")
text = "In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts."
# text = "This is the 10% Happier podcast. I'm Dan Harris. Hey, everybody. Today we're going to talk about a very common psychic ailment, perfectionism. Sometimes you hear people talk about perfectionism like it's a good thing, but that may be a bit of a misunderstanding in my view. It's great to have high standards. Of course, it's yet another thing to be so obsessive about the outcome of your work that you drive yourself and everybody around you totally nuts. Or to be so afraid of failure that you refuse to try anything new at all. In this episode, we're going to get some strategies for managing perfectionism from a very smart, successful person who has struggled mightily with perfectionism himself. Adam Grant is happy to say a frequent flier on this show. He's the number one New York Times bestselling author of five books that have sold millions of copies and been translated into 35 languages. Those books include Think Again, Give and Take Originals, Option B and Power Moves. He's an organizational psychologist who has been the top rated professor at Wharton for seven years in a row. He's also the host of a newish podcast, which everybody should go check out. It's called Rethinking with Adam Grant. And that's in addition to his other chart topping podcast called Work Life. In this conversation, we talked about Adam's definition of neurotic versus normal perfectionism and whether either is a healthy state of being. Why he thinks we're seeing a rise in perfectionism amongst younger people. Strategies for Managing Perfectionism. A different metric for measuring the quality of our work. The importance of finding the right judges of our work and reimagining our relationship to failure by setting a failure budget. That's his term. Then we pivot to a not unrelated issue procrastination and some strategies for managing that very common, very thorny problem."

doc = nlp(text)
noun_phrases = list(c.text for c in doc.noun_chunks)
print(noun_phrases)
print(len(noun_phrases))

query = vectorizer.transform(
    noun_phrases
)

##
import time

t1 = time.time()
matches = awesome_cossim_top(query, tf_idf_matrix.transpose(), 10, 0.85)
t = time.time() - t1
print("SELFTIMED:", t)

non_zeros = matches.nonzero()

sparserows = non_zeros[0]
sparsecols = non_zeros[1]

phrases = set()
for r, c in zip(sparserows, sparsecols):
    print(r, c)
    print(noun_phrases[r])
    print(org_names[c])
    # print(df[['label', 'description']].loc[c])
    # print(df[['label', 'description']].loc[c])
    phrases.add(noun_phrases[r])

print(noun_phrases)
print(phrases)

##
from lance.vector import vec_to_table

from scipy.sparse import csr_matrix

tf_idf_matrix: csr_matrix


def iterate_csr_matrix(sparse_matrix: csr_matrix, batch=512):
    for i in tqdm(range(0, sparse_matrix.shape[0], batch)):
        row_array = sparse_matrix[i:i + batch].toarray()
        yield tuple(range(i, i + batch)), row_array
        # for j, vector in zip(range(i, i+batch), row_array):
        #     yield j, vector


# print(vec_to_table(tf_idf_matrix[:128]))
# for i, vector in iterate_csr_matrix(tf_idf_matrix):
#     # print(i, vector.shape, vector)
#     # break
#     pass
#     break

from lance.vector import vec_to_table
from utils import run_function, combine_chunks

uri = "vec_data.lance"
for indices, rows in iterate_csr_matrix(tf_idf_matrix, batch=8192):
    # table = (dict(zip(indices, rows)))
    table = vec_to_table(dict(zip(indices, rows)), )
    # table = vec_to_table(rows)
    lance.write_dataset(table, uri, mode="append")

##
import time
import lance

vector = rows[0]
print(vector.shape, vector.sum(), indices[0])

start = time.time()
sift1m = lance.dataset(uri)
tbl = sift1m.to_table(columns=["id"], nearest={"column": "vector", "q": vector, "k": 8})
end = time.time()

print(f"Time(sec): {end - start}")
print(tbl.to_pandas())
for record in tbl.to_pandas().to_records():
    print(record[1], org_names[record[1]])

##
sift1m = lance.dataset(uri)
print(sift1m.schema)
sift1m.create_index("vector",
                    index_type="IVF_PQ",
                    num_partitions=512,  # IVF
                    num_sub_vectors=16,
                    metric='cosine',  # ?
                    )

##
import time
import lance
from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer

uri = "vec_data.lance"

vectorizer = HashingVectorizer(analyzer=ngrams, n_features=2 ** 13)

t1 = time.time()
sift1m = lance.dataset(uri)

noun_phrases = ['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell',
                'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine',
                'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow',
                'the McKnight Foundation', 'the Pew Charitable Trusts']
# noun_phrases = ['Pew Chair table Trusts',  'the McKnight Foundation']

results = {}
for phrase in noun_phrases:
    query = vectorizer.transform([phrase]).toarray()[0]
    results[phrase] = []
    tbl = sift1m.to_table(columns=["id"], nearest={"column": "vector", "q": query, "k": 5, "refine_factor": 5})
    for record in tbl.to_pandas().to_records():
        _, idx, _v, distance = record
        if distance < 0.5:
            results[phrase].append(org_names[idx])

t = time.time() - t1
print("SELFTIMED:", t)
print("SIZE:", sift1m.count_rows())
for phrase, result in results.items():
    if result:
        print(phrase, result)

# print(tbl.to_pandas())
# for record in tbl.to_pandas().to_records():
#     print(record[1], org_names[record[1]])

##
from utils import run_function

org_names_ngram = tuple(ngrams(w) for w in tqdm(org_names))

##
from typing import List
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

tagged_data: List[TaggedDocument] = [TaggedDocument(d, [i]) for i, d in enumerate(tqdm(org_names_ngram))]
print(org_names[:64])

##
import logging
# logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.CRITICAL)
logging.disable(logging.CRITICAL)

training_data = tagged_data[:1024 * 32 * 16]
model2 = Doc2Vec(vector_size=64, window=2, min_count=1, alpha=0.03)#, workers=16, alpha=0.015)
model2.build_vocab(training_data)
# model2.train(training_data, total_examples=model2.corpus_count, epochs=10)
# print("Done", model2.total_train_time)

epochs = []
for epoch in range(64):
    model2.train(training_data, total_examples=model2.corpus_count, epochs=1)
    print("Done", epoch, model2.total_train_time)

    test_doc = ngrams('Flora eta')
    test_doc_vector = model2.infer_vector(test_doc)
    b = model2.docvecs.most_similar(positive=[test_doc_vector], topn=32)

    for rank, (i, score) in enumerate(b):
        res = org_names[i]
        if res == "Flora Zeta":
            print(rank, round(score, 2), org_names[i])
            epochs.append([epoch, rank, round(score, 2), org_names[i]])

for e in epochs:
    print(e)
# EPOCH 8: training on 17932 raw words (18789 effective words) took 0.0s, 887113 effective words/s

# test_doc = ngrams("I had pizza and pasta")
# test_doc = ngrams("Pew Charitable Trust")
# test_doc = ngrams("health")
# test_doc = ngrams('WSL 2015 Samsung Galaxy Championship Tour')
# test_doc = ngrams('Flora eta')
# # test_doc = org_names_ngram[9]
# # test_doc = ngrams(word)
# # print(word)
# print(test_doc)
##
test_doc_vector = model2.infer_vector(test_doc)
b = model2.docvecs.most_similar(positive=[test_doc_vector])
# print(b)
#
for i, score in b:
    print(i, score, org_names[i])

##
import tensorflow_hub as hub

module_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
embed = hub.load(module_url)
##
start = time.time()
# m = embed(org_names[:1024*16*4])
for name in org_names[:1024*16*4]:
    m = embed([name])
end = time.time()

print(f"Time(sec): {end - start}", m.shape)

##
import numpy as np

m = embed(["Flora eta", "Flora Zeta"])
print(m.shape)

corr = np.inner(m, m)
print(corr)

##
from typing import Union

def iterate_chunks(array, batch=512):
    for i in tqdm(range(0, len(array), batch)):
        row_array = array[i:i + batch]
        yield tuple(range(i, i + batch)), row_array

# m = embed(org_names[:1024*16*4])
for indicies, chunk in iterate_chunks(org_names, 1024*16*4):
    embed(chunk)
