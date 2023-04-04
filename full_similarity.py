##
import lance

# dataset = lance.dataset("wikidata-5m.lance")
dataset = lance.dataset("wikidata-latest-86m.lance")

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
import pickle

vectorizer = pickle.load(open("vectorizer.pickle", "rb"))
reduction = pickle.load(open("reduction.pickle", "rb"))

 ##
from lance.vector import vec_to_table

def run(*columns):
    for item in dataset.to_table(columns=columns or None).to_batches():
        for row in item.to_pylist():
            yield row

def combine_chunks(chunks, item_count=1024 * 1024):
    result = []
    for item in chunks:
        result.append(item)
        if len(result) >= item_count:
            yield result
            result = []
    if result:
        yield result

org_names = (row['label'] for row in run('label'))

uri = "vec_data_full.lance"

chunk_size = 2 ** 20
current = 0
pbar = tqdm(combine_chunks(org_names, chunk_size), total=dataset.count_rows() // chunk_size)
for chunk in pbar:
    indices = range(current, current + len(chunk))
    current += len(chunk)
    rows = vectorizer.transform(chunk)
    rows = reduction.transform(rows)
    table = vec_to_table(dict(zip(indices, rows)), )
    pbar.desc = f'[{table.num_rows}, {len(chunk)}]'
    lance.write_dataset(table, uri, mode="append")
##
import lance
uri = "vec_data_full.lance"

sift1m = lance.dataset(uri)
print(sift1m.schema)
sift1m.create_index(
    "vector",
    index_type="IVF_PQ",
    num_partitions=512 * 8 * 2,  # IVF
    num_sub_vectors=16,
    # metric='cosine',  # ?
)
##
import time
import lance


t1 = time.time()
sift1m = lance.dataset(uri)

# noun_phrases = ['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell',
#                 'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine',
#                 'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow',
#                 'the McKnight Foundation', 'the Pew Charitable Trusts', 'Pew Charitable Trusts']
# noun_phrases += ['Pew Char table Trust',  'McKnight Foundation']
noun_phrases = ['This', 'the 10% Happier podcast', 'I', 'Dan Harris', 'Hey, everybody', 'we', 'a very common psychic ailment', 'perfectionism', 'you', 'people', 'perfectionism', 'it', 'a good thing', 'that', 'a bit', 'a misunderstanding', 'my view', 'It', 'high standards', 'it', 'another thing', 'the outcome', 'your work', 'you', 'yourself', 'everybody', 'you', 'failure', 'you', 'anything', 'this episode', 'we', 'some strategies', 'perfectionism', 'a very smart, successful person', 'who', 'perfectionism', 'Adam Grant', 'a frequent flier', 'this show', 'He', 'the number one New York Times bestselling author', 'five books', 'that', 'millions', 'copies', '35 languages', 'Those books', 'Originals', 'Option B', 'Power', 'Moves', 'He', 'an organizational psychologist', 'who', 'the top rated professor', 'Wharton', 'seven years', 'a row', 'He', 'the host', 'a newish podcast', 'which', 'everybody', 'It', 'Adam Grant', 'that', 'addition', 'his other chart topping podcast', 'Work Life', 'this conversation', 'we', "Adam's definition", 'normal perfectionism', 'a healthy state', 'he', 'we', 'a rise', 'perfectionism', 'younger people', 'Strategies', 'Managing Perfectionism', 'A different metric', 'the quality', 'our work', 'The importance', 'the right judges', 'our work', 'our relationship', 'failure', 'a failure budget', 'That', 'his term', 'we', 'a not unrelated issue procrastination', 'some strategies', 'that very common, very thorny problem']

results = {}
for phrase in noun_phrases:
    query = vectorizer.transform([phrase])
    query = reduction.transform(query)[0]

    results[phrase] = []
    tbl = sift1m.to_table(columns=["id"], nearest={"column": "vector", "q": query, "k": 5, "refine_factor": 25})
    for record in tbl.to_pandas().to_records():
        _, idx, _v, distance = record
        # if distance < 0.5:
        results[phrase].append(idx)
t = time.time() - t1
print("SELFTIMED:", t)
print("SIZE:", sift1m.count_rows())
for phrase, result in results.items():
    if result:
        result = dataset.take(result).to_pylist()
        print(phrase, [(e['id'], e['label']) for e in result])

##
import spacy

nlp = spacy.load("en_core_web_lg")
# text = "In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts."
text = "This is the 10% Happier podcast. I'm Dan Harris. Hey, everybody. Today we're going to talk about a very common psychic ailment, perfectionism. Sometimes you hear people talk about perfectionism like it's a good thing, but that may be a bit of a misunderstanding in my view. It's great to have high standards. Of course, it's yet another thing to be so obsessive about the outcome of your work that you drive yourself and everybody around you totally nuts. Or to be so afraid of failure that you refuse to try anything new at all. In this episode, we're going to get some strategies for managing perfectionism from a very smart, successful person who has struggled mightily with perfectionism himself. Adam Grant is happy to say a frequent flier on this show. He's the number one New York Times bestselling author of five books that have sold millions of copies and been translated into 35 languages. Those books include Think Again, Give and Take Originals, Option B and Power Moves. He's an organizational psychologist who has been the top rated professor at Wharton for seven years in a row. He's also the host of a newish podcast, which everybody should go check out. It's called Rethinking with Adam Grant. And that's in addition to his other chart topping podcast called Work Life. In this conversation, we talked about Adam's definition of neurotic versus normal perfectionism and whether either is a healthy state of being. Why he thinks we're seeing a rise in perfectionism amongst younger people. Strategies for Managing Perfectionism. A different metric for measuring the quality of our work. The importance of finding the right judges of our work and reimagining our relationship to failure by setting a failure budget. That's his term. Then we pivot to a not unrelated issue procrastination and some strategies for managing that very common, very thorny problem."

doc = nlp(text)
noun_phrases = {c.text.lower(): c.text for c in doc.noun_chunks}.values()
print(noun_phrases)
print(len(noun_phrases))
print(len(set(noun_phrases)))

##
items = []
for phrase, result in results.items():
    if result:
        result = dataset.take(result).to_pylist()
        items.append({
            "phrase": phrase,
            **result[0]
        })
        # print(phrase, [(e['id'], e['label']) for e in result])

items = sorted(items, key=lambda e: -e['label_count'])
items = {e['id']: e for e in items}.values()
for e in items:
    print(e)
##

