import numpy as np


def DCG(y):
    return (np.power(2, y) / np.log2(np.arange(2, y.size + 2))).sum()


def NDCG(y, k=5):
    max_dcg = DCG(np.sort(y)[::-1][:k])
    if max_dcg == 0:
        return 0
    return DCG(y[:k]) / max_dcg
