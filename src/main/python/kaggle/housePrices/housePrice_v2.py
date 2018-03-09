import pandas as pd
import seaborn as sns
import numpy as np
from scipy.stats import skew

# load data
train = pd.read_csv("D:/workspace/spark-tools/src/main/data/house-prices/train.csv")
test = pd.read_csv("D:/workspace/spark-tools/src/main/data/house-prices/test.csv")

# 观察数据
print(train.head())
a = train.loc[:,'MSSubClass':'SaleCondition']
b = test.loc[:,'MSSubClass':'SaleCondition']
# 取特征数据
all_data = pd.concat((train.loc[:,'MSSubClass':'SaleCondition'],
                      test.loc[:,'MSSubClass':'SaleCondition']))
# 对目标变量取log处理
print(train["SalePrice"].describe())
# 房价直方图分布
sns.distplot(train["SalePrice"])
price = pd.DataFrame({"price":train["SalePrice"],"price_log":np.log1p(train["SalePrice"])})
sns.distplot(np.log1p(train["SalePrice"]))

# 更新目标变量
train["SalePrice"] = np.log1p(train["SalePrice"])
# 计算偏度、log1p转化
numeric_feats = all_data.dtypes[all_data.dtypes != "object"].index
skewed_feats = train[numeric_feats].apply(lambda x:  skew(x.dropna()))
skewed_feats = skewed_feats[skewed_feats > 0.75]
skewed_feats = skewed_feats.index
all_data[skewed_feats] = np.log1p(all_data[skewed_feats])
all_data = pd.get_dummies(all_data)

# 缺失值用均值填补
all_data = all_data.fillna(all_data.mean())

# 构建训练数据
X_train = all_data[:train.shape[0]]
X_test = all_data[train.shape[0]:]
y = train.SalePrice

print("##############")