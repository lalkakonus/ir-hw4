import lightgbm as lgb
from data_combine import DataLoader
from matplotlib import pyplot as plt
import xgboost as xgb


class LightGBM:

    def __init__(self):
        self.model = None
        self.param = {'objective': 'lambdarank',
                      # 'boosting': 'gbdt',
                      'boosting': 'dart',
                      'learning_rate': 0.05,
                      # 'max_depth': 8,
                      # 'min_data_in_leaf': 200,
                      # 'num_leaves': 100,
                      # 'max_bin': 100
                      }

    def fit(self, train_data, validation_data, feature_names):
        self.eval = dict()
        self.model = lgb.train(self.param, train_data, num_boost_round=500,
                               valid_sets=[validation_data, train_data],
                               valid_names=["eval", "train"],
                               feature_name=feature_names,
                               evals_result=self.eval)
        lgb.plot_metric(self.eval)
        plt.show()

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

    def feature_importance(self):
        # lgb.plot_importance(self.model)
        lgb.plot_metric(self.model.evals_result_)
        # plt.show()

        plt.show()


class XGBoost:
    def __init__(self):
        self.model = xgb.Booster()
        self.params = {'objective': 'rank:ndcg',
                       'verbosity': 2,
                       'learning_rate': 0.05,
                       'gamma': 1.0,
                       'min_child_weight': 0.1,
                       'max_depth': 8,
                       'eval_metric': 'ndcg@5'}

    def fit(self, train_data, validation_data):
        self.model = xgb.train(self.params, train_data, num_boost_round=30,
                               evals=[(validation_data, "validation")])

    @staticmethod
    def get_data(dataloader):
        train_data, validation_data, test_data, feature_names = dataloader.load()
        X_train, y_train, groups_train = train_data
        X_validation, y_validation, groups_validation = validation_data
        X_test, _, _ = test_data

        train_dataset = xgb.DMatrix(X_train, label=y_train, feature_names=feature_names)
        train_dataset.set_group(groups_train)

        validation_dataset = xgb.DMatrix(X_validation, label=y_validation, feature_names=feature_names)
        validation_dataset.set_group(groups_validation)

        test_dataset = xgb.DMatrix(X_test, feature_names=feature_names)
        return train_dataset, validation_dataset, test_dataset

    def predict(self, test_data):
        return self.model.predict(test_data)

    def feature_importance(self):
        xgb.plot_importance(self.model)
        plt.show()


def main():
    dataloader = DataLoader(validation_part=0.05)

    # Light GBM
    lightGBM = LightGBM()
    train_data, validation_data, test_data, feature_names = lightGBM.get_data(dataloader)
    lightGBM.fit(train_data, validation_data, feature_names)
    result = lightGBM.predict(test_data)

    # XGBoost
    # xgboost = XGBoost()
    # train_data, validation_data, test_data = xgboost.get_data(dataloader)
    # xgboost.fit(train_data, validation_data)
    # result = xgboost.predict(test_data)

    dataloader.save(result)
    lightGBM.feature_importance()



if __name__ == "__main__":
    main()
