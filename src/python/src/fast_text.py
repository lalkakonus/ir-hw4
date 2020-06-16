import subprocess
from io import StringIO
from pathlib import Path
from progress.bar import Bar
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from gensim.models.keyedvectors import Word2VecKeyedVectors


class FastText:

    def __init__(self, mode="word",
                 model_path="../data/semantic/fasttext/cc.ru.300.bin",
                 fast_text_path="/home/sergey/fastText-0.9.2/fasttext"):
        self.pretrained_model_path = model_path
        self.fast_text_path = fast_text_path
        self.data_path_prefix = Path("../data")

        assert mode in {"sentence", "word", "nn"}
        self.mode = mode
        if self.mode == "word":
            self.path_postfix = "_sentence_embedding.tsv"
        elif mode == "sentence":
            self.path_postfix = "_word_embedding.kv"
        else:
            self.path_postfix = "_word_nn.tsv"

    def __str__(self):
        return "Embedding"

    def transform(self, data, output_path):
        if self.mode == "sentence":
            self._get_sentence_embedding(data, output_path)
        if self.mode == "word":
            self._get_word_embeddings(data, output_path)
        else:
            self._get_word_nn(data, output_path)

    def _get_sentence_embedding(self, data: pd.DataFrame, output_path):
        print("Embedding extraction started...")
        command = [self.fast_text_path, "print-sentence-vectors", self.pretrained_model_path]
        with output_path.open("w") as output_stream:
            process = subprocess.Popen(command,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       encoding="utf-8")

            for key, row in Bar("Sentence embedding progress", max=data.index.size).iter(data.iterrows()):
                process.stdin.write(row["text"] + "\n")
                process.stdin.flush()
                line = process.stdout.readline()
                output_stream.write("{} {}".format(key, line))

            process.stdin.close()
            process.wait()
        print("...finished. Result written to '{}'".format(output_path))

    def _get_word_embeddings(self, data, output_path, batch_size=7000):
        print("Word embedding extraction started...")
        dictionary = set()
        for _, row in data.iterrows():
            dictionary.update(row["text"].split())

        print("Total word count: {}".format(len(dictionary)))

        vectors = Word2VecKeyedVectors(vector_size=300)
        command = [self.fast_text_path, "print-word-vectors", self.pretrained_model_path]
        process = subprocess.Popen(command,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   encoding="utf-8")

        buffer = StringIO()
        for i, word in Bar("Word embedding progress", max=len(dictionary)).iter(enumerate(dictionary)):
            process.stdin.write(word + "\n")
            process.stdin.flush()
            buffer.write(process.stdout.readline())
            if i % batch_size == batch_size - 1:
                buffer.seek(0)
                data = np.genfromtxt(buffer)
                vectors.add(data[:, 0], data[:, 1:])
                buffer.flush()

        buffer.seek(0)
        data = np.genfromtxt(buffer)
        vectors.add(data[:, 0], data[:, 1:])

        process.stdin.close()
        process.wait()

        vectors.save(str(output_path))
        print("...finished. Result written to '{}'".format(output_path))
        return vectors

    def _get_word_nn(self, dictionary, output_path, k=5):
        print("Word nearest neighbour extraction started...")
        command = [self.fast_text_path, "nn", self.pretrained_model_path, str(k)]
        with output_path.open("w") as output_stream:
            process = subprocess.Popen(command,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       encoding="utf-8")

            for word in Bar("Word nearest neighbour progress", max=len(dictionary)).iter(dictionary):
                process.stdin.write(word + "\n")
                process.stdin.flush()
                for i in range(k - 1):
                    similar_word, similarity = process.stdout.readline().split()[-2:]
                    output_stream.write("{} {} {}".format(word, similar_word, similarity))

            process.stdin.close()
            process.wait()
        print("...finished. Result written to '{}'".format(output_path))

    def get_query_doc_similarities(self):
        path = self.data_path_prefix / "queries/queries_sentence_embeddings.txt"
        print("Queries embeddings loading from {} ...".format(path), end="")
        query_embeddings = np.loadtxt(path, ndmin=2)
        assert np.all(query_embeddings[:, 0] == np.arange(query_embeddings.shape[0]))
        query_embeddings = query_embeddings[:, 1:]
        print("OK")

        path = self.data_path_prefix / "titles/titles_sentence_embeddings.txt"
        print("Titles embeddings loading from {} ...".format(path), end="")
        title_embeddings = np.loadtxt(path, ndmin=2)
        assert np.all(title_embeddings[:, 0] == np.arange(title_embeddings.shape[0]))
        title_embeddings = title_embeddings[:, 1:]
        print("OK")

        relation = pd.read_csv(self.data_path_prefix / "config/samples.tsv", sep="\t")
        relation.drop(columns=["label", "sample_id"], inplace=True)
        relation = relation.assign(embedding_similarity=np.full(relation.index.size, 0.0))
        for key, group in Bar("Query doc embedding similarity progress",
                              max=relation.index.size).iter(relation.groupby("query_id")):
            doc_idx = group["doc_id"].values
            relation.loc[group.index, "embedding_similarity"] = cosine_similarity(query_embeddings[key].reshape(1, -1),
                                                                                  title_embeddings[doc_idx]).reshape(-1)

        path = self.data_path_prefix / "semantic/fasttext/query_doc_embedding_similarity.tsv"
        relation.to_csv(path, sep="\t", index=False)
        print("Result saved to file: {}".format(path))
        return relation

    def get_sim_queries(self, threshold=0.85):
        input_path = self.data_path_prefix / "queries/queries_sentence_embeddings.txt"
        print("Embeddings loading from {} ...".format(input_path), end="")
        query_embeddings = np.genfromtxt(input_path)
        assert np.all(query_embeddings[:, 0].astype(int) == np.arange(query_embeddings.shape[0]))
        query_embeddings = query_embeddings[:, 1:]
        print("OK")

        print("Calculate pairwise similarities ...", end="")
        similarities = cosine_similarity(query_embeddings)
        idx = np.argwhere(similarities > threshold)
        data = pd.DataFrame(idx, columns=["id_a", "id_b"])
        data = data.assign(similarity=similarities[similarities > threshold].reshape(-1))
        print("OK")

        output_path = self.data_path_prefix / "queries/similar_idx.tsv"
        data.to_csv(output_path, sep="\t", index=False)
        print("Result saved to file: {}".format(output_path))
        return data


def main():
    fastText = FastText()
    # fastText.get_query_doc_similarities()
    fastText.get_sim_queries()
    # df_0 = pd.read_csv("../data/similar_idx.tsv", sep="\t", dtype={"id_a": "int64", "id_b": "int64",
    #                                                                "score": "float"})
    # df_1 = pd.read_csv("../data/queries_spell_corrected.tsv", sep="\t")
    # idx = np.random.randint(0, df_0.index.size, 10)
    # for id in idx:
    #     id_a = int(df_0.iloc[id]["id_a"])
    #     id_b = int(df_0.iloc[id]["id_b"])
    #     score = df_0.iloc[id]["score"]
    #     print("({}) {} <-> {}".format(score, df_1.iloc[id_a].text, df_1.iloc[id_b].text))
    # print(df_0.index.size)


if __name__ == "__main__":
    main()