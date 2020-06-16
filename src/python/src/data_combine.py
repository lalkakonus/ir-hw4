from pathlib import Path
import pandas as pd
import numpy as np
from scipy.sparse import load_npz, csc_matrix, hstack, save_npz
import os


class Combiner:

    def __init__(self):

        self.prefix = Path("../data")

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
        prefix = self.prefix / "semantic/results"

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


class DataLoader:

    def __init__(self, validation_part=0.15):
        self.prefix = Path("../data/")
        self.validation_part = validation_part
        data = load_npz(self.prefix / "feature_matrix.npz")
        pure_data = data[:, 1:-2]
        self.feature_names = np.load(self.prefix / "feature_names.npy", allow_pickle=True)[1:-2].tolist()

        labels = np.asarray(data.getcol(data.shape[1] - 1).todense().astype(np.int)).reshape(-1)
        groups = np.asarray(data.getcol(data.shape[1] - 2).todense().astype(np.int)).reshape(-1)
        sample_id = np.asarray(data.getcol(0).todense().astype(np.int)).reshape(-1)
        assert np.all(sample_id == np.arange(sample_id.size))

        train_idx = np.argwhere(labels >= 0).reshape(-1)
        test_idx = np.argwhere(labels < 0).reshape(-1)

        self.x_train = pure_data[train_idx]
        self.labels_train = labels[train_idx]
        self.groups_train = self.get_groups(groups[train_idx])
        self.x_test = pure_data[test_idx]

    # def get_full_data(self):
    #     return self.pure_data

    @staticmethod
    def get_groups(groups_id):
        x = 0
        prev = groups_id[0]
        groups = []
        for group_id in groups_id:
            if group_id == prev:
                x += 1
            else:
                groups.append(x)
                x = 1
            prev = group_id
        groups.append(x)
        return groups

    def train_validation_split(self, X, y, groups, validation_part):
        k = int(len(groups) * validation_part)
        train_groups, valid_groups = groups[k:], groups[:k]
        train_sample_cnt = sum(train_groups)
        return X[:train_sample_cnt], X[train_sample_cnt:], y[:train_sample_cnt], y[train_sample_cnt:], \
               train_groups, valid_groups

    def load(self):
        x_train, x_valid, \
        y_train, y_valid, \
        groups_train, groups_valid = self.train_validation_split(self.x_train, self.labels_train,
                                                                 self.groups_train, self.validation_part)
        return (x_train, y_train, groups_train), \
               (x_valid, y_valid, groups_valid), \
               (self.x_test, None, None),\
               self.feature_names

    def save(self, prediction):
        dtypes = {"sample_id": "int", "query_id": "int", "doc_id": "int", "label": "float"}
        df = pd.read_csv(self.prefix / "config/samples.tsv", sep="\t", dtype=dtypes).set_index("sample_id")
        df = df[df["label"] < 0].sort_index().drop(columns="label").assign(score=prediction)
        # df = df.drop_duplicates(subset=["query_id", "doc_id"]).set_index("sample_id").sort_index()
        df.to_csv(self.prefix / "prediction/raw_predictions.tsv", sep="\t", index=False)


def main():
    combiner = Combiner()
    combiner.combine().save()
    # DataLoader().load()


if __name__ == "__main__":
    main()