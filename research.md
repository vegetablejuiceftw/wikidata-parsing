In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts.

https://huggingface.co/yanekyuk/bert-keyword-extractor?text=In+addition%2C+Andrew+consistently+publishes+his+original+research+findings+in+top+peer-reviewed+journals+like+Nature%2C+Cell%2C+Neuron%2C+and+Current+Biology.+His+work+has+been+featured+in+major+publications%2C+including+Science+magazine%2C+Discover+magazine%2C+Scientific+American%2C+Time%2C+and+the+New+York+Times.+He%E2%80%99s+also+a+regular+member+of+several+National+Institutes+of+Health+review+panels+and+is+a+Fellow+of+the+McKnight+Foundation+and+the+Pew+Charitable+Trusts.


https://huggingface.co/spaces/valurank/keyword_and_keyphrase_extraction

https://huggingface.co/valurank/MiniLM-L6-Keyword-Extraction

https://huggingface.co/Voicelab/vlt5-base-keywords?text=In+addition%2C+Andrew+consistently+publishes+his+original+research+findings+in+top+peer-reviewed+journals+like+Nature%2C+Cell%2C+Neuron%2C+and+Current+Biology.+His+work+has+been+featured+in+major+publications%2C+including+Science+magazine%2C+Discover+magazine%2C+Scientific+American%2C+Time%2C+and+the+New+York+Times.+He%E2%80%99s+also+a+regular+member+of+several+National+Institutes+of+Health+review+panels+and+is+a+Fellow+of+the+McKnight+Foundation+and+the+Pew+Charitable+Trusts.

https://huggingface.co/yanekyuk/bert-uncased-keyword-extractor?text=In+addition%2C+Andrew+consistently+publishes+his+original+research+findings+in+top+peer-reviewed+journals+like+Nature%2C+Cell%2C+Neuron%2C+and+Current+Biology.+His+work+has+been+featured+in+major+publications%2C+including+Science+magazine%2C+Discover+magazine%2C+Scientific+American%2C+Time%2C+and+the+New+York+Times.+He%E2%80%99s+also+a+regular+member+of+several+National+Institutes+of+Health+review+panels+and+is+a+Fellow+of+the+McKnight+Foundation+and+the+Pew+Charitable+Trusts.

https://www.kaggle.com/code/akhatova/extract-keywords
https://github.com/LIAAD/KeywordExtractor-Datasets

https://towardsdatascience.com/fuzzy-matching-at-scale-84f2bfd0c536

https://github.com/facebookresearch/faiss
https://github.com/neuml/txtai
https://github.com/currentslab/awesome-vector-search
https://github.com/qdrant/qdrant_client
https://github.com/qdrant/qdrant
https://github.com/milvus-io/milvus
https://github.com/weaviate/weaviate
https://github.com/eto-ai/lance


```python
tokens = [
"nature",
"cell",
"neuron",
"current biology",
"science magazine",
"discover magazine",
"scientific american",
"time",
"the new york times",
"national institutes of health",
"mcknight foundation",
"pew charitable trusts",
]
# Andrew Andrew, Neuron, Neuron, Neuron, Neuron, Neuron, Neuro

```

```commandline
                     label           sitelink   sitebadges  sitelink_count  label_count                                     description                                                                                              aliases
2        George Washington  George Washington  [Q17437798]  234             219          1st president of the United States (1732−1799)  [Washington, President Washington, G. Washington, Father of the United States, The American Fabius]
4017443  George Washington  None               []           0               2            painting by Rembrandt Peale  (CAM 1884.365)     []  
```

zgrep  -m 1 'Pew Charitable' wikidata-20220103-all.json.gz

['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell', 'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine', 'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow', 'the McKnight Foundation', 'the Pew Charitable Trusts']
{'Health', 'Nature', 'Time', 'addition', 'Science magazine', 'Cell', 'the New York Times', 'Andrew', 'He'}


Field(id=1, name=label, type=string)
Field(id=2, name=sitelink, type=string)
Field(id=3, name=sitebadges, type=list)
Field(id=5, name=sitelink_count, type=int64)
Field(id=6, name=label_count, type=int64)
Field(id=7, name=description, type=string)
Field(id=8, name=aliases, type=list)

Field(id=1, name=label, type=string)
Field(id=2, name=sitelink, type=string)
Field(id=3, name=sitebadges, type=list)
Field(id=5, name=sitelink_count, type=int64)
Field(id=6, name=label_count, type=int64)
Field(id=7, name=description, type=string)
Field(id=8, name=aliases, type=list)

['addition', 'Andrew', 'his original research findings', 'top peer-reviewed journals', 'Nature', 'Cell', 'Neuron', 'Current Biology', 'His work', 'major publications', 'Science magazine', 'Discover magazine', 'Scientific American', 'Time', 'the New York Times', 'He', 'a regular member', 'Health', 'a Fellow', 'the McKnight Foundation', 'the Pew Charitable Trusts']
{'Discover magazine', 'top peer-reviewed journals', 'Nature', 'the Pew Charitable Trusts', 'Cell', 'Andrew', 'Neuron', 'He', 'Health', 'Scientific American', 'Time', 'the McKnight Foundation', 'Science magazine'}

[
  "Belgium",
  "happiness",
  "George Washington",
  "Jack Bauer",
  "Douglas Adams",
  "Paul Otlet",
  "Wikidata",
  "Portugal",
  "Antarctica",
  "penis",
  ...
  "Salvan",
  "Johannes Max Proskauer",
  "Princess Theresa of Bavaria",
  "Mézières",
  "Morrens",
  "Kamarhati",
  "Jürgen Bartsch",
  "1952 Formula One season",
  "Cheolsan Station",
  "Dernancourt"
]


query = vectorizer.transform(['Pew Chair table Trusts'])
(8192,) [0. 0. 0. ... 0. 0. 0.]
SELFTIMED: 0.03431272506713867
SIZE: 4 813 491 terms

        id                                             vector     score
0   524043  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.700080
1  1626324  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.860937
2   447564  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.864631
3   816505  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.870163
4  2576666  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.880469
5    67857  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.892497
6  2474983  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.903950
7  1686758  [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ...  0.910420
524043 Pew Charitable Trust
1626324 table table
447564 4 Noble Truths
816505 Table tap
2576666 Pebble TV
67857 Przedwojów
2474983 esmée fairbairn charitable trust
1686758 Bubble hair



SELFTIMED: 0.7192625999450684
SIZE: 4813491
Andrew ['AndrewAndrew', 'Sandrew', 'andrews', 'andrewe', 'mandrews']
top peer-reviewed journals ['peer reviewed journal']
Nature ['Naturex', 'naturen', 'naturely', 'xignature', 'Nature contre nature']
Cell ['cellé', 'cell', 'tcell', 'ACELL', 'Cell7']
Neuron ['Neuron', 'Synneuron', 'Neuronium']
Current Biology ['Current Biology Magazine']
major publications ['publications', 'TSAR Publications', 'jmir publications', 'star publications', 'bb publications']
Science magazine ['conscience (magazine)', 'fence (magazine)', 'Audience (magazine)', 'science magzine', 'Popular Science (magazine)']
Discover magazine ['Discover Magazine', 'cover (magazine)']
Scientific American ['scientific america', 'unscientific america', 'scientific american mind']
Time ['time2', 'mtime']
the New York Times ['The New York Times Upfront', 'Play (New York Times)', 'The New York Times Crosswords']
Health ['xlhealth', 'health pei', 'health.com', 'HealthSat', 'PopHealth']
the McKnight Foundation ['the mcknight foundation', 'Knight Foundation']
the Pew Charitable Trusts ['Pew Charitable Trust']


#@title Load the Universal Sentence Encoder's TF Hub module
from absl import logging

import tensorflow as tf

import tensorflow_hub as hub
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re
import seaborn as sns

module_url = "https://tfhub.dev/google/universal-sentence-encoder/4" #@param ["https://tfhub.dev/google/universal-sentence-encoder/4", "https://tfhub.dev/google/universal-sentence-encoder-large/5"]
model = hub.load(module_url)
print ("module %s loaded" % module_url)
def embed(input):
  return model(input)

10_000 titles -> 0.6s