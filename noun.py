import spacy

nlp = spacy.load("en_core_web_lg")

text = "In addition, Andrew consistently publishes his original research findings in top peer-reviewed journals like Nature, Cell, Neuron, and Current Biology. His work has been featured in major publications, including Science magazine, Discover magazine, Scientific American, Time, and the New York Times. He’s also a regular member of several National Institutes of Health review panels and is a Fellow of the McKnight Foundation and the Pew Charitable Trusts."

doc = nlp(text)

noun_phrases = list(doc.noun_chunks)
for np in noun_phrases:
    print(np)

print()
import re
doc = nlp(re.sub(r"[,.]", "", text).lower())

noun_phrases = list(doc.noun_chunks)
for np in noun_phrases:
    print(np)
