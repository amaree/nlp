# -*- coding: utf-8 -*-
import sys
from multiprocessing import Pool
import csv

words_to_ignore = ["that","what","with","this","would","from","your","which","while","these","i","to","a","are","the"]

def Map(L):
 
  results = []
  for w in L:
    # True if w contains non-alphanumeric characters
    if not w.isalnum():
      w = sanitize(w)

    results.append((w, 1))
 
  return results
 
def Partition(L):
  tf = {}
  for sublist in L:
    for p in sublist:
      # Append the tuple to the list in the map
      try:
        tf[p[0]].append(p)
      except KeyError:
        tf[p[0]] = [p]
  return tf
 
def Reduce(Mapping):
  return (Mapping[0], sum(pair[1] for pair in Mapping[1]))

def sanitize(word):

    while len(word) > 0 and not word[0].isalnum():
        word = word[1:]

    while len(word) > 0 and not word[-1].isalnum():
        word = word[:-1]

    return word

""" Load a file into a big string of words """
def load(path):
    
    word_list = []
    with open(path, encoding="utf8",errors="ignore") as input:
        reader = csv.reader(input)
        for row in reader:
            if (row[0] is not ''):
                rowArray = set(row[0].split(' '))
                filtered_words = rowArray.difference(words_to_ignore)
                word_list.extend(filtered_words) 

    return word_list

""" Generator for splitting out words """
def genWords(l,n):
    step = int(n) or 1
    for i in range(0, len(l), step):
        yield l[i:i + int(step)]

def WriteTopWordsCSV(file, values):
    with open(file, 'w', encoding="utf8", errors="ignore") as out:
        writer = csv.writer(out)
        writer.writerows(values)

if __name__ == '__main__':

    if(len(sys.argv) != 2):
        print("Need to provide a Path parameter")
        sys.exit(1)

    text = load(sys.argv[1])

    pool = Pool(processes=8,)

    partitioned_text = list(genWords(text, len(text) / 8))

    single_count_tuples = pool.map(Map, partitioned_text)

    token_to_tuples = Partition(single_count_tuples)

    term_frequencies = pool.map(Reduce, token_to_tuples.items())

    term_frequencies.sort(key=lambda tup: tup[1], reverse=True)

    WriteTopWordsCSV('topwords.csv', term_frequencies)

    for pair in term_frequencies[:20]:
        print(pair[0], ":", pair[1])