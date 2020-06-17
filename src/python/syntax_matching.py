from gensim.corpora import Dictionary
from gensim.models import TfidfModel as GensimTfIdf
from gensim.similarities import SparseMatrixSimilarity
from gensim.summarization.bm25 import BM25 as GensimBM25
from helper import NDCG
import msgpack
import numpy as np
import pandas as pd
from pathlib import Path


class TfIdf:

    def __init__(self, data_folder):
        self.model_path = data_folder / "tf_idf_model.npz"
        self.index_path = data_folder / "tf_idf_index.npz"
        self.dictionary_path = data_folder / "dictionary_index.npz"

        self.model = None
        self.index = None
        self.dictionary = None

    def fit(self, bow_corpus, dictionary, smartirs="ltc"):
        self.dictionary = dictionary
        print("TF_IDF:\tFit model...", end="")
        self.model = GensimTfIdf(corpus=bow_corpus, smartirs=smartirs, slope=1.0)
        print("OK\nTF_IDF:\tBuilding search index...", end="")
        self.index = SparseMatrixSimilarity(self.model[bow_corpus], num_features=len(self.dictionary.dfs))
        print("OK")
        return self

    def predict(self, tokenized_query, doc_idx):
        vectorized_query = self.dictionary.doc2bow(tokenized_query)
        td_idf_query = self.model[vectorized_query]
        return self.index[td_idf_query][doc_idx]

    def load(self):
        print("TF_IDF:\tLoad model and index...", end="")
        self.model = GensimTfIdf.load(str(self.model_path))
        self.index = SparseMatrixSimilarity.load(str(self.index_path))
        self.dictionary = Dictionary.load(str(self.dictionary_path))
        print("OK")
        return self

    def save(self):
        print("TF_IDF:\tSave model and index...", end="")
        self.model.save(str(self.model_path))
        self.index.save(str(self.index_path))
        self.dictionary.save(str(self.dictionary_path))
        print("OK")
        return self


class BM25:

    def __init__(self):
        self.model = None

    def fit(self, corpus, k1=1.5, b=0.75):
        print("BM25:\tFit model...", end="")
        self.model = GensimBM25(corpus, k1=k1, b=b)
        print("OK")
        return self

    def predict(self, tokenized_query, doc_idx):
        return [self.model.get_score(tokenized_query, doc_id) for doc_id in doc_idx]


class Gensim:

    def __init__(self, data_folder):
        self.dictionary_path = data_folder / "dictionary.npz"
        self.corpus_path = data_folder / "corpus.mp"
        self.bow_corpus_path = data_folder / "bow_corpus.mp"

        self.dictionary = Dictionary()
        self.bow_corpus = []
        self.corpus = []
        self.tf_idf = TfIdf(data_folder)
        self.bm_25 = BM25()

    def BuildCorpora(self, data):
        print("Corpus building started...")
        self.corpus = data
        print("\tCreating corpus dictionary...", end="")
        self.dictionary = Dictionary(self.corpus)
        print("OK\n\tCreated corpus bow representation...", end="")
        self.bow_corpus = [self.dictionary.doc2bow(document) for document in self.corpus]
        print("OK\nCorpus building finished")
        return self

    def saveCorpora(self):
        print("Saving words corpus...", end="")
        self.dictionary.save(str(self.dictionary_path))
        with self.corpus_path.open("wb") as output_file:
            output_file.write(msgpack.packb(self.corpus))
        with self.bow_corpus_path.open("wb") as output_file:
            output_file.write(msgpack.packb(self.bow_corpus))

        print("OK")
        return self

    def loadCorpus(self):
        print("Loading corpus data...", end="")
        self.dictionary = Dictionary.load(str(self.dictionary_path))
        with self.corpus_path.open("rb") as input_file:
            self.corpus = msgpack.load(input_file)
        with self.bow_corpus_path.open("rb") as input_file:
            self.bow_corpus = msgpack.load(input_file)
        print("OK")
        return self

    def fit_tf_idf(self):
        self.tf_idf.fit(self.bow_corpus, self.dictionary)
        return self.tf_idf

    def fit_bm_25(self):
        self.bm_25.fit(self.corpus)
        return self


def main():
    mode = "word_2"
    assert mode in {"char_3", "word", "word_2", "char_4"}
    prefix = Path("data")
    title_input_path = prefix / "titles/titles_{}_tokenized.pkl".format(mode)
    tokenized_titles_df = pd.read_pickle(title_input_path)
    print("Titles data loaded from '{}'".format(title_input_path))
    gensim = Gensim(data_folder=prefix / "syntax" / mode)
    gensim.BuildCorpora(tokenized_titles_df["text"].to_list()).saveCorpora()
    gensim.loadCorpus()
    gensim.fit_bm_25()
    gensim.tf_idf.fit(gensim.bow_corpus, gensim.dictionary).save()
    gensim.tf_idf.load()

    tokenized_queries_df = pd.read_pickle(prefix / "queries/queries_{}_tokenized.pkl".format(mode))
    relation = pd.read_csv(prefix / "config/samples.tsv", sep="\t", index_col=0)
    output_path = prefix / "syntax" / mode / "scores.tsv"

    labelled_queries = relation[relation["label"] > 0]["query_id"].unique()
    ndcg = np.zeros(shape=(2, labelled_queries.size))
    k, m, N = 0, 0, relation["query_id"].unique().size

    with open(output_path, "w") as output:
        output.write("{}\t{}\t{}\t{}\n".format("query_id", "doc_id", "tf_idf_score", "bm_25_score"))
        for query_id, group in relation.groupby("query_id"):
            m += 1
            doc_idx = group["doc_id"].values

            tfi_df_score = gensim.tf_idf.predict(tokenized_queries_df.loc[query_id]["text"], doc_idx)
            bm_25_score = gensim.bm_25.predict(tokenized_queries_df.loc[query_id]["text"], doc_idx)

            if group["label"].min() >= 0:
                ndcg[0, k] = NDCG(group["label"].values[np.argsort(tfi_df_score)[::-1]])
                ndcg[1, k] = NDCG(group["label"].values[np.argsort(bm_25_score)[::-1]])
                k += 1
                print("TF-IDF NDCG: {:05.3f}\t BM-25 "
                      "NDCG: {:05.3f} \t{}/{}\r".format(*ndcg[:, :k].mean(axis=1), m, N), end="")
            for i, doc_id in enumerate(doc_idx):
                output.write("{}\t{}\t{}\t{}\n".format(query_id, doc_id, tfi_df_score[i], bm_25_score[i]))

    print("\nResult saved to '{}'".format(output_path))


if __name__ == "__main__":
    main()
