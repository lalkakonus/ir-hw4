from fast_text import FastText
import pandas as pd
import numpy as np
from pathlib import Path
from progress.bar import Bar
import math

# fastText = FastText()
# fastText.get_sim_queries()
# df_1 = pd.read_csv("../data/queries_spell_corrected.tsv", sep="\t")
# idx = np.random.randint(0, df_0.index.size, 10)
# for id in idx:
#     id_a = int(df_0.iloc[id]["id_a"])
#     id_b = int(df_0.iloc[id]["id_b"])
#     score = df_0.iloc[id]["score"]
#     print("({}) {} <-> {}".format(score, df_1.iloc[id_a].text, df_1.iloc[id_b].text))
# print(df_0.index.size)
class Collaborative:

    def __init__(self):
        # self.min = 0
        self.prefix = Path("../data")
        self.raw_prediction = pd.read_csv(self.prefix / "prediction/raw_predictions.tsv", sep="\t")
        with open(self.prefix / "prediction/ranged_prediction.csv", "w") as output_file:
            output_file.write("DocumentId,QueryId\n")

    def update_relation(self):
        FastText().get_sim_queries(threshold=0.83)

        similarity_df = pd.read_csv("../data/similar_idx.tsv", sep="\t", dtype={"id_a": "int64", "id_b": "int64",
                                                                                "score": "float"})
        relation_df = pd.read_csv("../data/query_docs_relation.tsv", sep="\t", index_col=0)
        upgraded_relation = relation_df.copy(deep=True)
        for key, row in similarity_df.iterrows():
            id_a = int(row["id_a"])
            id_b = int(row["id_b"])
            upgraded_relation.loc[id_a, "doc_ids"] += "," + relation_df.loc[id_b, "doc_ids"]
        upgraded_relation.to_csv("../data/upgraded_query_docs_relation.tsv", sep="\t")

    def funtciton(self, x, min_threshold=0.83, max_threshold=0.99, k=2.5):
        # numerator = x - min_threshold
        # denominator = max_threshold - min_threshold
        # value = math.pow(numerator / denominator, k)
        if x > 0.85:
            value = 6 * x - 5
        else:
            value = 0
        return max(0, min(value, 1))

    def get_scores(self):
        similarity_df = pd.read_csv("../data/similar_idx.tsv", sep="\t", dtype={"id_a": "int64", "id_b": "int64",
                                                                                "score": "float"}).rename(columns={"score": "similarity"})
        relation_df = pd.read_csv("../data/raw_predictions.tsv",
                                  sep="\t").set_index(["query_id", "doc_id"]).drop(columns="sample_id")

        raw_relation_df = pd.read_csv("../data/raw_samples.tsv",
                                      sep="\t")
        test_query_ids = set(raw_relation_df[raw_relation_df.label < 0]["query_id"])
        raw_relation_df = raw_relation_df.set_index(["query_id", "doc_id"]).drop(columns="sample_id")

        self.min = similarity_df["similarity"].min()
        # relation_df = relation_df[relation_df["label"] < 0]

        # relation_df["label"] = scores
        t = 0
        for key, group in Bar("Sentence embedding progress",
                              max=similarity_df["id_a"].unique().size).iter(similarity_df.groupby("id_a")):
            if key not in test_query_ids:
                continue
            queries = raw_relation_df.loc[key].index
            scores = np.zeros(queries.size)
            group["similarity"] = group["similarity"].apply(self.funtciton)
            for _, row in group.iterrows():
                scores += row["similarity"] * relation_df.loc[row["id_b"]].loc[queries].values.ravel()
            if group.index.size > 1:
                t += 1
            scores /= group["similarity"].sum()
            self.dump_scores(scores, key, queries)
        print(t)

    def dump_scores(self, scores, query_id, doc_ids):
        with open(self.prefix / "prediction/ranged_prediction.csv", "a") as output_file:
            idx = np.argsort(scores)[::-1][:5]
            for doc_id in doc_ids[idx]:
                output_file.write("{},{}\n".format(doc_id, query_id))


        # df = df[df["label"] < 0].sort_index()
        # query_ids = df["query_id"].values.reshape(-1)
        # doc_ids = df["doc_id"].values.reshape(-1)
        #
        # with open("../data/ranged_prediction.csv", "w") as output_file:
        #     output_file.write("DocumentId,QueryId\n")
        #     for query_id in range(query_ids.min(), query_ids.max() + 1):
        #         mask = query_ids == query_id
        #         idx = np.argsort(prediction[mask])[::-1][:5]
        #         for doc_id in doc_ids[mask][idx]:
        #             output_file.write("{},{}\n".format(doc_id, query_id))

    def plain_result(self):
        for key, group in self.raw_prediction.groupby("query_id"):
            self.dump_scores(group["score"].values, key, group["doc_id"].values)


def main():
    collaborative = Collaborative()
    # collaborative.get_scores()
    collaborative.plain_result()

if __name__ == "__main__":
    main()
