import pandas as pd
import numpy as np
from pandas import Series,DataFrame
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

df_train = pd.read_csv("D:/workspace/spark-tools/src/main/data/house-prices/train.csv")

# 目标变量勘探
print(df_train["SalePrice"].describe())
# 房价直方图分布
sns.distplot(df_train["SalePrice"])

# relationship with numerical variables
# 散点图 探索数字变量与因变量的关联关系
data = df_train[["SalePrice","GrLivArea"]]
data.plot.scatter(x="GrLivArea",y="SalePrice",ylim=(0,800000))

# 箱型图 探索类别变量与因变量的关联关系
# f, ax = plt.subplots(figsize=(8, 6))
sns.boxplot(x="OverallQual", y="SalePrice", data=df_train)

# correlation matrix 全变量相关性分析
corrmat = df_train.corr()
sns.heatmap(corrmat,vmax= 0.8,square=True)

# salePrice 相关性分析
k = 10
cols = corrmat.nlargest(k, 'SalePrice')['SalePrice'].index
cm = np.corrcoef(df_train[cols].values.T)
sns.set(font_scale=1.25)
hm = sns.heatmap(cm, cbar=True, annot=True, square=True, fmt='.2f', annot_kws={'size': 10}, yticklabels=cols.values, xticklabels=cols.values)
# plt.show()

# missing data 缺失值数据处理
# 缺失值排序，isnull 返回判断值true和false
total = df_train.isnull().sum().sort_values(ascending=False)
# 缺失值占比
percent = (df_train.isnull().sum()/df_train.isnull().count()).sort_values(ascending=False)
missing_data = pd.concat([total, percent], axis=1, keys=['Total', 'Percent'])
missing_data.head(20)

# dealing with missing data
col = (missing_data[missing_data['Total'] > 1]).index
df_train = df_train.drop((missing_data[missing_data['Total'] > 1]).index,1)
df_train = df_train.drop(df_train.loc[df_train['Electrical'].isnull()].index)
# f_train.isnull().sum().max()

# 离群点数据分析处理
# 通过散点图做二元分析 bivariate analysis

# 数据标准化
# saleprice_scaled = StandardScaler().fit_transform(df_train['SalePrice'][:,np.newaxis])

# deleting points
a = df_train.sort_values(by = 'GrLivArea', ascending = False)[:2]
# 删除top2的离群点数据
df_train = df_train.drop(df_train[df_train['Id'] == 1299].index)
df_train = df_train.drop(df_train[df_train['Id'] == 524].index)

print("##########")
