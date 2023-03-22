##
import lance
import pandas as pd

dataset = lance.dataset("wikidata.lance")
# dataset = lance.dataset("test.lance")
df: pd.DataFrame = dataset.to_table().to_pandas()

print(df.shape, df.columns)
with pd.option_context('display.max_columns', None, 'max_colwidth', -1, 'display.width', 1000):
    print(df)

##
import re
from ftfy import fix_text

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm.auto import tqdm

def ngrams(string, n=4):
    # string = str(string)
    string = fix_text(string)  # fix text
    string = string.encode("ascii", errors="ignore").decode()  # remove non ascii chars
    string = string.lower()
    # chars_to_remove = []
    # rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
    string = re.sub(r"[)(.|\[\]{}']", '', string)
    string = re.sub(r'[,-./]\s*', ' ', string)

    string = string.replace('&', 'and')
    # string = string.title()  # normalise case - capital at start of each word
    # string = re.sub(' +', ' ', string).strip()  # get rid of multiple spaces and replace with a single
    # string = ' ' + string + ' '  # pad names for ngrams...
    # string = re.sub(r'[,-./]|\sBD', r'', string)
    # ngrams = zip(*[string[i:] for i in range(n)])
    # return tuple(''.join(ngram) for ngram in ngrams)
    # string += "  "
    return tuple(
        string[i:i+n]
        for i in range(0, max(len(string) - 2, 1))
    )


print('All 3-grams in "Department":')
print(ngrams('Department Pants'))
print(ngrams('OK'))

##
org_names = df['label']#.unique()
print(org_names.shape)
print(org_names.unique().shape)

print("start")
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix = vectorizer.fit_transform(tqdm(org_names))
print("done")
# print(tf_idf_matrix)

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
t = time.time()-t1
print("SELFTIMED:", t)
# print(matches)

non_zeros = matches.nonzero()

sparserows = non_zeros[0]
sparsecols = non_zeros[1]

for r, c in zip(sparserows, sparsecols):
    print(r, c)
    print(org_names[r])
    print(org_names[c+100])

##
with pd.option_context('display.max_columns', None, 'max_colwidth', -1, 'display.width', 1000):
    print(df.loc[[2, 4017343+100]])

##
import spacy

nlp = spacy.load("en_core_web_lg")
text = "In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts."

doc = nlp(text)
noun_phrases = list(c.text for c in doc.noun_chunks)
print(noun_phrases)

query = vectorizer.transform(
    noun_phrases
)

##
import time
t1 = time.time()
matches = awesome_cossim_top(query, tf_idf_matrix.transpose(), 10, 0.85)
t = time.time()-t1
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
for i, name in enumerate(tqdm(org_names)):
    if "the Pew".lower() in name.lower():
        print(i, name)