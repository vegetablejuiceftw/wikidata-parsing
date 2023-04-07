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



"https://tfhub.dev/google/universal-sentence-encoder/4"
10_000 title embeddings -> 0.6s  
86000000 / 10000 * 0.6 = 5160s = 1.43 hours (CPU)
SELFTIMED: 0.1330699920654297
SIZE: 4813491
addition ['Nucleophillic addition']
Andrew ['Andrew!!!', 'andrew bathgate', 'andrew checketts', 'andrew northcote', 'Andrew Sherborne']
top peer-reviewed journals ['berghahn journals', 'NASW Press Journals', 'Journals']
Cell ['cell', 'cellule', 'cell metab', 'Cell Ausaille', 'Mesangial cell']
Neuron ['Neuron', 'Multipolar neuron', 'Preganglionic neuron', 'unipolar neurons']
Current Biology ['classis (biology)', 'feminisation (biology)', 'wikipedia:wikiproject biology']
major publications ['FGH Publications', 'ansariyan publications', 'micromedia publications', 'Tharpa Publications', 'bookcraft publications']
Science magazine ['Science World (magazine)', 'Science Reporter (magazine)', 'Science of Mind Magazine', 'Popular Science (magazine)']
Discover magazine ['Discover Magazine', 'Discover Magazine (TV series', 'explore (magazine)']
Scientific American ['scientific america']
Time ['Time, Time', 'time in place', 'Le Temps', 'time river/sandbox', 'du temps']
He ['.it', 'i.t', 'Il Lavoratore', 'Lui', 'Il Politecnico']
the McKnight Foundation ['the mcknight foundation', 'The Kelton Foundation', 'the surfrider foundation', 'the womanity foundation', 'the ditchley foundation']
Pew Charitable Trusts ['Pew Charitable Trust']

for comparison
20 x [t for t in tqdm(org_names) if "pew c" in t.lower()] = 15 sec

https://arctype.com/blog/postgres-fuzzy-search/


in-memory  awesome_cossim_top , hashvectorizer
SELFTIMED: 0.776176929473877
Andrew ['AndrewAndrew', 'andrewe', 'Andrews, Andrew', 'Sandrew', 'andrews']
top peer-reviewed journals ['peer reviewed journal']
Nature ['Naturex', 'naturen']
Cell ['cellé', 'cell']
Neuron ['Neuron']
Science magazine ['conscience (magazine)']
Discover magazine ['Discover Magazine', 'cover (magazine)']
Scientific American ['scientific america', 'unscientific america', 'scientific american mind']
Time ['Time, Time']
He ['Öhe']
the McKnight Foundation ['the mcknight foundation']
the Pew Charitable Trusts ['Pew Charitable Trust']
Pew Charitable Trusts ['Pew Charitable Trust']

tfidf
SELFTIMED: 0.6489496231079102
Andrew ['AndrewAndrew', 'Sandrew', 'Andrews, Andrew']
top peer-reviewed journals ['peer reviewed journal']
Nature ['naturen']
Cell ['cellé', 'cell', 'CELL セル']
Neuron ['Neuron', 'Neuronia', 'ReNeuron']
Science magazine ['conscience (magazine)']
Discover magazine ['Discover Magazine', 'cover (magazine)']
Scientific American ['scientific america', 'scientific american mind', 'unscientific america']
Time ['TiMER', 'Time, Time']
He ['Öhe']
Health ['e-health']
the McKnight Foundation ['the mcknight foundation']
the Pew Charitable Trusts ['Pew Charitable Trust']
Pew Charitable Trusts ['Pew Charitable Trust']


# tf-idf pca 128 
SELFTIMED: 0.1604759693145752
SIZE: 4813491
addition ['Fruition', 'Xpedition', 'PM Edition', 'ArsEdition', 'ygnition']
Andrew ['AndrewAndrew', 'andrei', 'andrews', 'Andrezé', 'andrewe']
his original research findings ['Original Recipe (EP)', 'cecil reginald burch', 'Final Reckoning', '12x12 original remixes', 'Original Plumbing']
top peer-reviewed journals ['peer reviewed journal', 'Fjölnir (journal)', 'fafnir (journal)', 'prooftexts (journal)', 'glq (journal)']
Nature ['Naturex', 'naturely', 'naturéo', 'xignature', 'Naturi']
Cell ['cell', 'cellé', 'Pell', 'kellé', 'mellwell']
Neuron ['Neuron', 'beuron', 'dieuron', 'kuron', 'muron']
Current Biology ['Current 218', 'swift current, sk', 'Brent Fultz', 'Brent Kosolofski', 'Concurrent Euclid']
His work ['this funny world', "Tarski's World", 'Autism is a world', 'This Tiny World', 'This World Fair EP']
major publications ['jmir publications', 'Fox Publications', 'Orbit Publications', 'bv publications', 'ICC Publications']
Science magazine ['Audience (magazine)', 'fence (magazine)', 'dance magazine', 'essence magazine', 'Moondance (magazine)']
Discover magazine ['Discover Magazine', 'cover (magazine)', 'Revolver magazine', 'Overflow magazine', 'Believer Magazine']
Scientific American ['scientific america', 'unscientific america', 'Agent: America', 'scenic america', 'turkic american']
Time ['DTIME', 'mtime', 'mytime', 'time 3', 'time2']
the New York Times ['The New York Fund', 'the new york age', 'The New York Globe', 'The New You!', 'The New York Pops']
He ['Věteřov', 'teoh', 'Teok', 'Teočak', 'ATSF 3415']
a regular member ['Maya Rehberg', 'Petr Reinberk', 'Beru Revue', 'Rolf Reber', 'ludwig reiber']
Health ["heal's", 'healthful', 'healthkit', 'xlhealth', 'zynx health']
a Fellow ['Elloes', 'Qello', 'XellOs', 'Pelloe', 'Ellogos']
the McKnight Foundation ['the mcknight foundation', 'The Twilight Zone/Execution', 'the four knights', 'the midnight folk', 'the night before zipsmas (jungle junction)']
the Pew Charitable Trusts ['Pew Charitable Trust', 'LHA Charitable Trust', 'esmée fairbairn charitable trust', 'Rajiv Gandhi Charitable Trust', 'panacea charitable trust']
Pew Charitable Trusts ['Pew Charitable Trust', 'LHA Charitable Trust', 'Rajiv Gandhi Charitable Trust', 'esmée fairbairn charitable trust', 'panacea charitable trust']

# 96m entries
SELFTIMED: 1.0625929832458496
SIZE: 86962534
addition [('Q32043', 'addition'), ('Q353204', 'addition'), ('Q43111453', 'Addition.'), ('Q48783573', 'Addition'), ('Q28036028', 'Addition')]
Andrew [('Q4065799', 'Andrew'), ('Q111905430', 'Andrew'), ('Q114507595', 'Andrew'), ('Q7509664', 'Andrew'), ('Q114539448', 'Andrew')]
his original research findings [('Q59167536', 'Original Research'), ('Q60465340', 'Original Research'), ('Q1813536', 'original research'), ('Q70867427', 'Research findings'), ('Q55527801', 'Very original research.')]
top peer-reviewed journals [('Q43104343', 'Peer-reviewed journals.'), ('Q33616093', 'A peer-reviewed journal?'), ('Q107307831', 'FIG Peer Review Journal'), ('Q96695499', 'A Peer-Reviewed Journal About'), ('Q38030741', "What's a peer reviewed journal?")]
Nature [('Q113852591', 'Nature'), ('Q49156570', 'Nature.'), ('Q107196626', 'nature'), ('Q180445', 'Nature'), ('Q348620', 'Nature')]
Cell [('Q20505218', 'Cell'), ('Q2349697', 'Cell'), ('Q863823', 'Cell'), ('Q3664265', 'Cell'), ('Q109502754', 'Cell')]
Neuron [('Q43054', 'neuron'), ('Q415060', 'Neuron'), ('Q7002467', 'Neuron'), ('Q107306040', 'Neuron'), ('Q57265286', 'Neuron')]
Current Biology [('Q80506473', 'Current biology'), ('Q83414976', 'Current biology'), ('Q1144851', 'Current Biology'), ('Q80506239', 'Current biology'), ('Q15749150', 'Current Zoology')]
His work [('Q108298349', "Dei's World"), ('Q85784904', 'Memphis World'), ('Q96328859', 'GIS World'), ('Q64707422', "Petzi's World"), ('Q585710', 'This World')]
major publications [('Q6738289', 'Major Publications'), ('Q3061805', 'Excelsior Publications'), ('Q111308734', 'CHSPR Publications'), ('Q7099986', 'Orb Publications'), ('Q52636577', 'ACG Publications')]
Science magazine [('Q113391982', 'Science Magazine'), ('Q3475759', 'Science magazine'), ('Q7433565', 'Science Magazine'), ('Q63718105', 'Law Science Magazine'), ('Q96735482', 'The Egyptian Science Magazine')]
Discover magazine [('Q5281778', 'Discover Magazine'), ('Q5179164', 'Cover Magazine'), ('Q5179165', 'Cover Magazine'), ('Q103355909', 'Cover Magazine'), ('Q7113656', 'Overflow magazine')]
Scientific American [('Q81854337', 'SCIENTIFIC AMERICAN'), ('Q39379', 'Scientific American'), ('Q19869428', 'Coptic American'), ('Q15946385', 'Turkic American'), ('Q16191961', 'Tajik American')]
Time [('Q74429827', 'Time'), ('Q81083294', 'Time'), ('Q107324744', 'Time'), ('Q57551858', 'Time'), ('Q19105284', 'Time')]
the New York Times [('Q9684', 'The New York Times'), ('Q108864820', 'Not The New York Times'), ('Q33184098', 'The New York Times ad.'), ('Q81565726', 'New York Times'), ('Q116890502', 'New York Times')]
He [('Q25459', '4+'), ('Q24845', '76'), ('Q29779', '3T'), ('Q24836', '85'), ('Q30409', '75')]
a regular member [('Q11349517', 'Regular Member'), ('Q106465794', 'Popular Members'), ('Q64509195', 'Nanular Member'), ('Q1826416', 'Regular number'), ('Q100698449', 'A Regular Guy')]
Health [('Q19798707', 'Health'), ('Q12147', 'health'), ('Q93644472', 'Health'), ('Q45783286', 'Health.'), ('Q81147897', 'Health')]
a Fellow [('Q63487913', 'A fellow'), ('Q24853885', 'Ba Fello'), ('Q57682009', 'CSA Fellow'), ('Q108678788', 'IF Fellows'), ('Q103927524', 'Odd Fellow')]
the McKnight Foundation [('Q6802026', 'McKnight Foundation'), ('Q7640493', 'Sunlight Foundation'), ('Q16801251', 'Knight Foundation'), ('Q55230423', 'LifeFlight Foundation'), ('Q30296836', 'Fulbright Foundation')]
the Pew Charitable Trusts [('Q201296', 'The Pew Charitable Trusts'), ('Q54912542', 'The Kt Wong Charitable Trust'), ('Q25440909', 'The Immutable Truth'), ('Q20862018', 'Mumble-the-Peg'), ('Q89121139', 'The Puzzle Table')]
Pew Charitable Trusts [('Q111469378', 'Anubhuti Charitable Trust'), ('Q111469952', 'Resq Charitable Trust'), ('Q111469864', 'Om Charitable Trust'), ('Q45135672', 'EORTC Charitable Trust'), ('Q5074440', 'charitable trust')]
Pew Char table Trust [('Q26925044', 'Kūh-e Taghar Chāl'), ('Q924595', 'Char Aznable'), ('Q28228677', 'Dhar Taycha'), ('Q5994618', 'Ihar Truhaw'), ('Q6063466', 'Nehar Tüblek')]
McKnight Foundation [('Q6802026', 'McKnight Foundation'), ('Q16801251', 'Knight Foundation'), ('Q7640493', 'Sunlight Foundation'), ('Q55230423', 'LifeFlight Foundation'), ('Q30296836', 'Fulbright Foundation')]


QID: label dmb
wiki-5m -> 500MB, 50_000it/s
wiki-latest -> 10GB, somehow 5 hours? gets slower over time?


--- /home/fred/PycharmProjects/pythonProject --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  294,0 GiB [##########] /.data                                                                                                                                                                                                                                                                                        
  168,5 GiB [#####     ] /vec_data_full.lance
  166,5 GiB [#####     ] /vec_data_full.lance (copy)
   17,4 GiB [          ] /wikidata-latest.beta.lance
   12,7 GiB [          ]  dict.dbm
   12,5 GiB [          ] /wikidata-latest-86m.lance
   11,9 GiB [          ] /wikidata-20220103-backup.lance
    9,7 GiB [          ]  dict-index.dbm
    9,3 GiB [          ] /vec_data_idf_pca3.lance
    9,3 GiB [          ] /vec_data_u.lance
    4,7 GiB [          ] /vec_data_idf_pca2.lance
    2,5 GiB [          ] /vec_data_idf_pca.lance
    2,5 GiB [          ] /vec_data_pca.lance
  766,5 MiB [          ] /wikidata2.lance
  737,1 MiB [          ] /wikidata-5m.lance
  685,2 MiB [          ] /wikidata.lance
  469,3 MiB [          ] /wikidata-latest-5percent.lance
  243,1 MiB [          ]  wikidata2.pp
  243,1 MiB [          ]  wikidata.pp
  172,3 MiB [          ]  wikidata.mp


https://github.com/wolfgarbe/SymSpell
https://symspellpy.readthedocs.io/en/latest/examples/lookup_compound.html

https://pypi.org/project/msgpack/
https://github.com/capnproto/pycapnp/blob/master/examples/addressbook.capnp
https://github.com/eto-ai/lance
https://flatbuffers.dev/flatbuffers_guide_tutorial.html
https://pytrie.readthedocs.io/en/latest/
https://pypi.org/project/DAWG/0.8.0/
https://github.com/oborchers/Fast_Sentence_Embeddings

https://github.com/mchaput/whoosh
https://whoosh.readthedocs.io/en/latest/spelling.html “Did you mean... ?” Correcting errors in user queries
https://github.com/quickwit-oss/tantivy
https://github.com/valeriansaliou/sonic
https://github.com/zincsearch/zincsearch


-----

# DBM
0.0009s for 120 wiki-id queries on 86m item map.  
5 hours to generate (bug?)

# PDICT
import time
0.0018s to 120 wiki-id queries on 86m item map.  
9 minutes to generate
