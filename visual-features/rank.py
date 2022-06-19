import cv2
import numpy as np
import os


img_paths = sorted(os.listdir('imgs'))

vecs1 = np.load('densenet161_vecs.npy')
vecs2 = np.load('resnet50_vecs.npy')
vecs = np.concatenate((vecs2, vecs1), axis=1)  # ensemble
vecs /= np.sqrt(np.power(vecs, 2).sum(-1))[..., np.newaxis]  # normalize
similarity = vecs @ vecs.T  # cosine similarity

k = 20
# 1:k+1 below because the first one is the original image itself
all_top = np.argsort(-similarity)[:, 1:k+1]


def get_aid(img_path):
    return img_path.split('.')[0]


with open('visual_similar.txt', 'w+') as fi:
    for img_path, top_ids in zip(img_paths, all_top):
        fi.write(f"{get_aid(img_path)}:{','.join([get_aid(img_paths[i]) for i in top_ids])}\n")

