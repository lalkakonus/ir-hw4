from pathlib import Path
import pandas as pd
import numpy as np
from scipy.sparse import load_npz, csc_matrix, hstack, save_npz
import os


class Combiner:

    def __init__(self):

        self.prefix = Path("data")

        dtypes = {"sample_id": "int", "query_id": "int", "doc_id": "int", "label": "int"}
        self.relation_df = pd.read_csv(self.prefix / "config/samples.tsv", sep="\t", dtype=dtypes)

        self.semantic_features = self.relation_df.copy(deep=True).drop(columns=["label", "sample_id"]).set_index(["query_id", "doc_id"])
        self.syntax_features = self.relation_df.copy(deep=True).drop(columns=["label", "sample_id"]).set_index(["query_id", "doc_id"])

        self.feature_matrix = None
        self.feature_names = []

    def combine(self):
        self.combine_semantic_features()
        self.combine_syntax_features()

        relation_df = self.relation_df.copy(deep=True).drop(columns=["label"]).set_index(["query_id", "doc_id"])
        dense_features = relation_df.merge(self.semantic_features, left_index=True, right_index=True)
        dense_features = dense_features.merge(self.syntax_features, left_index=True, right_index=True)
        dense_features = dense_features.set_index("sample_id").sort_index()
        relation_df = self.relation_df.copy(deep=True).drop(columns=["doc_id"]).set_index(["sample_id"])
        dense_features = dense_features.merge(relation_df, left_index=True, right_index=True)

        idx = dense_features.index.values
        input_path = self.prefix / "click/click_statistic.npz"
        print("Loading click statistic from '{}'...".format(input_path), end="")
        click_statistic = load_npz(input_path)
        click_statistic_header = np.load(self.prefix / "click/click_statistic_header.npy")
        assert np.all(click_statistic[idx].getcol(0).data.astype(np.int) == dense_features.sort_index().index.values)
        print("OK\n Merging with semantic and syntax data...", end="")
        self.feature_matrix = hstack((click_statistic[idx], csc_matrix(dense_features)))
        self.feature_names = np.hstack((click_statistic_header, dense_features.columns.values))
        print("OK")
        return self

    def save(self):
        matrix_output_path = str(self.prefix / "feature_matrix.npz")
        save_npz(matrix_output_path, self.feature_matrix)
        print("Feature matrix saved to {}".format(matrix_output_path))

        names_output_path = str((self.prefix / "feature_names.npy"))
        np.save(names_output_path, self.feature_names)
        print("Feature names saved to {}".format(names_output_path))

    def combine_semantic_features(self):
        prefix = self.prefix / "semantic"

        for name in os.listdir(prefix):
            input_path = prefix / name
            print("Loading semantic features from '{}'...".format(input_path), end="")
            dtypes = {"query_id": "int32", "doc_id": "int32"}
            features_df = pd.read_csv(prefix / name,
                                      sep="\t", dtype=dtypes).set_index(["query_id", "doc_id"])
            column_prefix = name[:name.find("_query_doc_embedding_similarity")] + "_"
            features_df.rename(columns={x: column_prefix + x for x in features_df.columns}, inplace=True)
            print("OK\n Merging with other...", end="")
            self.semantic_features = self.semantic_features.merge(features_df, left_index=True, right_index=True)
            print("OK")

    def combine_syntax_features(self):
        prefix = self.prefix / "syntax"

        for name in os.listdir(prefix):
            input_path = prefix / name / "scores.tsv"
            dtypes = {"query_id": "int32", "doc_id": "int32"}
            print("Loading syntax features from '{}'...".format(input_path), end="")
            features_df = pd.read_csv(input_path,
                                      sep="\t", dtype=dtypes).set_index(["query_id", "doc_id"])
            features_df.rename(columns={x: name + "_" + x for x in features_df.columns}, inplace=True)
            print("OK\n Merging with other...", end="")
            self.syntax_features = self.syntax_features.merge(features_df, left_index=True, right_index=True)
            print("OK")


def main():
    combiner = Combiner()
    combiner.combine().save()


if __name__ == "__main__":
    main()