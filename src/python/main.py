import lightgbm as lgb
from dataloader import Loader
from matplotlib import pyplot as plt


class LightGBM:

    def __init__(self):
        self.model = None
        self.param = {'objective': 'lambdarank',
                      'boosting': 'dart',
                      'learning_rate': 0.04,
                      'force_col_wise': True
                      }

    def fit(self, train_data, validation_data, feature_names):
        self.eval = dict()
        self.model = lgb.train(self.param, train_data, num_boost_round=6,
                               valid_sets=[validation_data, train_data],
                               valid_names=["eval", "train"],
                               feature_name=feature_names,
                               evals_result=self.eval)

    @staticmethod
    def get_data(dataloader):
        train_data, validation_data, test_data, feature_names = dataloader.load()
        X_train, y_train, groups_train = train_data
        X_validation, y_validation, groups_validation = validation_data
        X_test, _, _ = test_data

        train_dataset = lgb.Dataset(X_train, label=y_train, group=groups_train)
        validation_dataset = lgb.Dataset(X_validation, label=y_validation, group=groups_validation,
                                         reference=train_dataset)
        return train_dataset, validation_dataset, X_test, feature_names

    def predict(self, test_data):
        return self.model.predict(test_data)

    def plot_training_curve(self):
        lgb.plot_metric(self.model.evals_result_)
        plt.show()

    def feature_importance(self):
        lgb.plot_importance(self.model)
        plt.show()


def main():
    loader = Loader(validation_part=0.15)

    lightGBM = LightGBM()
    train_data, validation_data, test_data, feature_names = lightGBM.get_data(loader)
    lightGBM.fit(train_data, validation_data, feature_names)
    predictions = lightGBM.predict(test_data)
    loader.save(predictions)


if __name__ == "__main__":
    main()
