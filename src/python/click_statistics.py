from pathlib import Path
from scipy.sparse import csc_matrix, save_npz,load_npz
import numpy as np
import pandas as pd


class Helper:

    def __init__(self):
        self.prefix = Path("../data/click")
        self.input_data_path = self.prefix / "part-r-00000"
        self.output_data_path = self.prefix / "click_statistic.npz"

        KEY_CNT = 25
        KEY_STATE_CNT = 5
        SAMPLE_CNT = pd.read_csv(self.prefix / "../config/samples.tsv", sep="\t").index.size
        FEATURES_CNT = KEY_CNT * KEY_STATE_CNT + 1
        self.data_shape = [SAMPLE_CNT, FEATURES_CNT]

        _header = np.array(["IMPLEMENTATION_DOC_KEY", "CLICK_DOC_KEY", "CTR_KEY", "AVG_POS_DIFF_LAST_DOC",
                            "AVG_POS_DIFF_NEXT_DOC", "FIRST_CTR_KEY", "LAST_CTR_KEY", "ONLY_CTR_KEY",
                            "PERCENT_CLICK_QUERY_DOC_KEY", "AVG_VIEW_TIME_KEY", "AVG_POSITION_KEY",
                            "AVG_CLICK_POSITION_KEY", "AVG_CLICK_NUMBER_KEY", "AVG_INVERSE_CLICK_NUMBER_KEY",
                            "AVG_NUMBER_CLICKED_BEFORE_KEY", "AVG_NUMBER_CLICKED_AFTER_KEY", "PROB_LAST_CLICKED_KEY",
                            "PROB_UP_CLICK_KEY", "PROB_DOWN_CLICK_KEY", "PROB_DOUBLE_CLICK_KEY",
                            "PROB_COME_BACK_KEY", "PROB_GO_BACK_KEY", "AVG_WORK_TIME_QUERY_KEY",
                            "AVG_FIRST_POSITION_QUERY_KEY", "AVG_CLICK_COUNT_QUERY_KEY"])
        sub = ["URL_QUERY", "HOST_URL_QUERY", "QUERY", "URL", "HOST"]
        self.header = ["sample_id", ]
        for name in _header:
            for update in sub:
                self.header.append(name + "(" + update + ")")
        self.header = np.array(self.header)

    def transform_data(self):
        self._transform_dataset(self.input_data_path, str(self.output_data_path), self.data_shape)

    def _transform_dataset(self, input_path, output_path, shape):
        x = []
        y = []
        data = []

        print("Start file '{}' process".format(input_path))
        with open(input_path, "r") as raw_data:
            for line in raw_data:
                sample_number, sample_data = line.split("\t")
                for item in sample_data.split(","):
                    feature_number, feature_value = item.split(":")
                    y.append(int(sample_number))
                    x.append(int(feature_number) + 1)
                    data.append(float(feature_value))

        for sample_id in range(shape[0]):
            y.append(sample_id)
            x.append(0)
            data.append(sample_id)
        click_statistics = csc_matrix((data, [y, x]), shape)

        idx = []
        for i in range(shape[1]):
            if click_statistics.getcol(i).count_nonzero() > 0:
                idx.append(i)

        click_statistics = click_statistics[:, idx]
        self.header = self.header[idx]

        np.save(self.prefix / "click_statistic_header.npy", self.header)
        print("Header saved to '{}'".format(self.prefix / "click_statistic_header.npy"))

        save_npz(output_path, click_statistics)
        print("Process finished. Result saved to '{}'".format(output_path))


def main():
    Helper().transform_data()


if __name__ == "__main__":
    main()
