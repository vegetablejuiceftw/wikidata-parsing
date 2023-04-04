##
import lance

dataset = lance.dataset("wikidata-5m.lance")
# dataset = lance.dataset("wikidata-latest-86m.lance")

##
import re
from ftfy import fix_text
import numpy as np
from tqdm.auto import tqdm


def ngrams(string, n=3):
    string = fix_text(string)  # fix text
    string = string.encode("ascii", errors="ignore").decode()  # remove non ascii chars
    string = string.lower()
    string = re.sub(r"[)(.|\[\]{}']", '', string)
    string = re.sub(r'[,-./]\s*', ' ', string)
    string = string.replace('&', 'and')
    if n is None:
        return string
    return tuple(
        string[i:i + n]
        for i in range(0, max(len(string) - 2, 1))
    )


print('All 3-grams in "Department":')
print(ngrams('Department of Pants'))
print(ngrams('OK'))
print(ngrams('Department (of) Pants', n=None))


##
def run(*columns):
    for item in dataset.to_table(columns=columns or None).to_batches():
        for row in item.to_pylist():
            yield row


org_names = tuple(row['label'] for row in run('label'))

##
from sklearn.feature_extraction.text import HashingVectorizer, TfidfVectorizer

print("start")
vectorizer = HashingVectorizer(analyzer=ngrams, n_features=2 ** 15)
tf_idf_matrix = vectorizer.fit_transform(tqdm(org_names, total=dataset.count_rows()))
print("done")

##
from sklearn.feature_extraction.text import HashingVectorizer, TfidfVectorizer

print("start")
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams, dtype=np.float32)
tf_idf_matrix = vectorizer.fit_transform(tqdm(org_names, total=dataset.count_rows()))
print("done")

##
# tf_idf_matrix_np = tf_idf_matrix.toarray()

##
import spacy

nlp = spacy.load("en_core_web_lg")
text = "In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts."
# text = "This is the 10% Happier podcast. I'm Dan Harris. Hey, everybody. Today we're going to talk about a very common psychic ailment, perfectionism. Sometimes you hear people talk about perfectionism like it's a good thing, but that may be a bit of a misunderstanding in my view. It's great to have high standards. Of course, it's yet another thing to be so obsessive about the outcome of your work that you drive yourself and everybody around you totally nuts. Or to be so afraid of failure that you refuse to try anything new at all. In this episode, we're going to get some strategies for managing perfectionism from a very smart, successful person who has struggled mightily with perfectionism himself. Adam Grant is happy to say a frequent flier on this show. He's the number one New York Times bestselling author of five books that have sold millions of copies and been translated into 35 languages. Those books include Think Again, Give and Take Originals, Option B and Power Moves. He's an organizational psychologist who has been the top rated professor at Wharton for seven years in a row. He's also the host of a newish podcast, which everybody should go check out. It's called Rethinking with Adam Grant. And that's in addition to his other chart topping podcast called Work Life. In this conversation, we talked about Adam's definition of neurotic versus normal perfectionism and whether either is a healthy state of being. Why he thinks we're seeing a rise in perfectionism amongst younger people. Strategies for Managing Perfectionism. A different metric for measuring the quality of our work. The importance of finding the right judges of our work and reimagining our relationship to failure by setting a failure budget. That's his term. Then we pivot to a not unrelated issue procrastination and some strategies for managing that very common, very thorny problem."

doc = nlp(text)
noun_phrases = list(c.text for c in doc.noun_chunks)
print(noun_phrases)
print(len(noun_phrases))

##
import time
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

noun_phrases = ['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell',
                'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine',
                'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow',
                'the McKnight Foundation', 'the Pew Charitable Trusts', 'Pew Charitable Trusts']
# noun_phrases = ['the Pew Charitable Trust',  'McKnight Foundation']

database = tf_idf_matrix
query = vectorizer.transform(noun_phrases)
# print(type(query))

t1 = time.time()
matches = awesome_cossim_top(query, database.transpose(), 5, 0.85)
t = time.time() - t1
print("SELFTIMED:", t)

non_zeros = matches.nonzero()

sparserows = non_zeros[0]
sparsecols = non_zeros[1]

results = {n: [] for n in noun_phrases}
for r, c in zip(sparserows, sparsecols):
    results[noun_phrases[r]].append(org_names[c])
for phrase, result in results.items():
    if result:
        print(phrase, result)



##
from sklearn.decomposition import PCA, TruncatedSVD

reduction = TruncatedSVD(n_components=512)
reduction_matrix = reduction.fit_transform(tf_idf_matrix)

##
reduction_matrix[0].toarray()

##
from lance.vector import vec_to_table

def iterate_chunks(array, batch=512):
    for i in tqdm(range(0, len(array), batch)):
        row_array = array[i:i + batch]
        yield tuple(range(i, i + batch)), row_array

uri = "vec_data_idf_pca3.lance"
for indices, rows in iterate_chunks(reduction_matrix, batch=8192):
    table = vec_to_table(dict(zip(indices, rows)), )
    lance.write_dataset(table, uri, mode="append")

sift1m = lance.dataset(uri)
print(sift1m.schema)
sift1m.create_index(
    "vector",
    index_type="IVF_PQ",
    num_partitions=512,  # IVF
    num_sub_vectors=16,
    metric='cosine',  # ?
)
##
import time
import lance


t1 = time.time()
sift1m = lance.dataset(uri)

noun_phrases = ['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell',
                'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine',
                'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow',
                'the McKnight Foundation', 'the Pew Charitable Trusts', 'Pew Charitable Trusts']
noun_phrases += ['Pew Char table Trust',  'McKnight Foundation']

results = {}
for phrase in noun_phrases:
    query = vectorizer.transform([phrase])
    query = reduction.transform(query)[0]

    results[phrase] = []
    tbl = sift1m.to_table(columns=["id"], nearest={"column": "vector", "q": query, "k": 5, "refine_factor": 25})
    for record in tbl.to_pandas().to_records():
        _, idx, _v, distance = record
        # if distance < 0.5:
        results[phrase].append(org_names[idx])

t = time.time() - t1
print("SELFTIMED:", t)
print("SIZE:", sift1m.count_rows())
for phrase, result in results.items():
    if result:
        print(phrase, result)


##
import pickle

pickle.dump(vectorizer, open("vectorizer.pickle", "wb"))
pickle.dump(reduction, open("reduction.pickle", "wb"))
