# -*- coding: utf-8 -*-
"""bert_description.ipynb

Automatically generated by Colaboratory.
"""

import os
import json
from time import sleep
from pprint import pprint

inp_file = 'raw.json'
out_file = 'description.json'
with open(inp_file) as f:
    data = json.load(f)['Items']
    
description = []
ids = []
max_lenght = 0
for item in data:
    aid = item['attractionId']['S']

    raw_attraction_types = item['attractionType']['S']

    raw_desc = raw_attraction_types+'. '+item['description']['S']
    max_lenght = max(max_lenght, len(raw_desc.split()))
    description.append(raw_desc)
    ids.append(aid)
print(max_lenght)

print(len(description))

from transformers import AutoTokenizer
model_name = "bert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)

from transformers import AutoModel
pt_model = AutoModel.from_pretrained(model_name)

from tqdm import tqdm
embeddings=[]

for value in tqdm(description):
  pt_batch = tokenizer(
    value,
    padding=True,
    truncation=True,
    max_length=256,
    return_tensors="pt",
  )
  pt_outputs = pt_model(**pt_batch)
  embedding = pt_outputs.pooler_output.squeeze()
  embeddings.append(embedding.tolist())

from sklearn.neighbors import NearestNeighbors
import numpy as np
embeddings = np.array(embeddings)
nbrs = NearestNeighbors(n_neighbors=10, algorithm='ball_tree').fit(embeddings)
distances, indices = nbrs.kneighbors(embeddings)

print(indices[0])

L = []
for item in indices:
  s = ids[item[0]] + ':'
  for idx in item[1:-1]:
    s+= ids[idx] + ','
  s+= ids[item[-1]]+' \n'
  L.append(s)

for att in indices[0]:
  print(description[att])

file1 = open("description_similarity.txt","w")
file1.writelines(L)
file1.close()
