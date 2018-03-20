import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 1、数据加载
# 2、数据勘探
# 3、因变量分布勘探，平滑处理
# 4、单变量分布勘探
# 5、缺失值处理
# 6、异常值处理
# 7、相关性分析
# 8、变量筛选
# 9、模型训练
# 10、模型评估

train_data = pd.read_csv("D:/workspace/spark-tools/src/main/data/house-prices/train.csv")
test_data = pd.read_csv("D:/workspace/spark-tools/src/main/data/house-prices/test.csv")

category_features = train_data.dtypes[train_data.dtypes == "object"].index
numeric_features = train_data.dtypes[train_data.dtypes != "object"].index

t = train_data[category_features].mode().iloc[0]
a = train_data[numeric_features].mean()
print(train_data[numeric_features].fillna(a))
# hah = train_data[category_features]

for cols in train_data.columns:
    if train_data[cols].dtype == np.object:
        train_data = pd.concat((train_data, pd.get_dummies(train_data[cols], prefix=cols)), axis=1)
        del train_data[cols]
print(train_data)

from sklearn.ensemble import RandomForestRegressor
# 按照特征重要性, 进行降序排列, 最重要的特征在最前面
etr = RandomForestRegressor(n_estimators=400)
train_y = train_data["SalePrice"]
train_x = train_data.drop(['SalePrice'], axis=1)
etr.fit(train_x, train_y)
imp = pd.DataFrame({'feature': train_x.columns, 'score': etr.feature_importances_})
result_feature = imp[(imp['score'] > 0.0005)]["feature"]
print(result_feature.values.tolist())

