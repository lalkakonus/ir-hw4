from pathlib import Path
import pandas as pd
import numpy as np
from scipy.sparse import load_npz


class Loader:

    def __init__(self, validation_part=0.15):
        data_directory = Path("data/")
        self.validation_part = validation_part
        self.feature_matrix_path = data_directory / "combined_data/feature_matrix.npz"
        self.feature_names_path = data_directory / "combined_data/feature_names.npy"
        self.samples_data_path = data_directory / "config/samples.tsv"
        self.prediction_path = data_directory / "prediction.csv"

        for path in [self.feature_matrix_path, self.feature_names_path, self.samples_data_path]:
            if not path.exists():
                raise Exception("Error: Path '{}' doesn't exists.".format(path))
        self.prediction_header = "DocumentId,QueryId\n"
        self.samples_data_types = {"sample_id": "int", "query_id": "int", "doc_id": "int", "label": "float"}

    @staticmethod
    def _get_groups(groups_id):
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

    def _train_validation_split(self, X, y, groups):
        k = int(len(groups) * self.validation_part)
        train_groups, valid_groups = groups[k:], groups[:k]
        train_sample_cnt = sum(train_groups)

        train_data = (X[:train_sample_cnt], y[:train_sample_cnt], train_groups)
        validation_data = (X[train_sample_cnt:], y[train_sample_cnt:], valid_groups)
        return train_data, validation_data

    def load(self):
        print("Loading dataset from '{}'...".format(self.feature_matrix_path), end="", flush=True)
        data = load_npz(self.feature_matrix_path)
        print("OK")
        pure_data = data[:, 1:-2]

        feature_names = np.load(self.feature_names_path, allow_pickle=True)[1:-2].tolist()

        labels = np.asarray(data.getcol(data.shape[1] - 1).todense().astype(np.int)).reshape(-1)
        groups = np.asarray(data.getcol(data.shape[1] - 2).todense().astype(np.int)).reshape(-1)
        sample_id = np.asarray(data.getcol(0).todense().astype(np.int)).reshape(-1)
        assert np.all(sample_id == np.arange(sample_id.size))

        train_idx = np.argwhere(labels >= 0).reshape(-1)
        test_idx = np.argwhere(labels < 0).reshape(-1)

        x_train = pure_data[train_idx]
        labels_train = labels[train_idx]
        groups_train = self._get_groups(groups[train_idx])
        x_test = pure_data[test_idx]

        train_data, validation_data = self._train_validation_split(x_train, labels_train, groups_train)
        test_data = (x_test, None, None)
        return train_data, validation_data, test_data, feature_names

    def save(self, prediction):
        samples_data = pd.read_csv(self.samples_data_path, sep="\t", dtype=self.samples_data_types)
        samples_data = samples_data[samples_data["label"] < 0].drop(columns="label")
        samples_data = samples_data.set_index("sample_id").sort_index().assign(score=prediction)

        print("Grouping and sorting predictions...", end="", flush=True)
        with open(self.prediction_path, "w") as output_file:
            output_file.write(self.prediction_header)
            for key, group in samples_data.groupby("query_id"):
                idx = np.argsort(group["score"].values)[::-1][:5]
                for doc_id in group["doc_id"].values[idx]:
                    output_file.write("{},{}\n".format(doc_id, key))
        print("OK\nResult saved to '{}'".format(self.prediction_path))
