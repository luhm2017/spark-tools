import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import skew

# 线性归回模型  Regularized Linear Models
# https://www.kaggle.com/apapiu/regularized-linear-models

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
# 线性回归的前提假设条件，因变量y符合正态分布，此处做转换--平滑处理
# 预测结果需要反转 expm1函数
price = pd.DataFrame({"price":train["SalePrice"],"price_log":np.log1p(train["SalePrice"])})
sns.distplot(np.log1p(train["SalePrice"]))

# 更新目标变量
train["SalePrice"] = np.log1p(train["SalePrice"])

# 计算偏度
numeric_feats = all_data.dtypes[all_data.dtypes != "object"].index
# 所有数值型变量，计算偏度skew
skewed_feats = train[numeric_feats].apply(lambda x:  skew(x.dropna()))
skewed_feats = skewed_feats[skewed_feats > 0.75]
skewed_feats = skewed_feats.index
# 针对正态分布的偏度比较大的数值型变量，做平滑转换处理
all_data[skewed_feats] = np.log1p(all_data[skewed_feats])
# one-hot处理，只针对类别变量有效
all_data = pd.get_dummies(all_data)
# 缺失值用均值填补
all_data = all_data.fillna(all_data.mean())

# 构建训练数据
X_train = all_data[:train.shape[0]]
X_test = all_data[train.shape[0]:]
y = train.SalePrice

# 训练模型
from sklearn.linear_model import Ridge, LassoCV
from sklearn.model_selection import cross_val_score

# 交叉验证模型，交叉验证评分
def rmse_cv(model):
    # neg_mean_squared_error 负的均方误差值，rmse均方根误差
    rmse= np.sqrt(-cross_val_score(model, X_train, y, scoring="neg_mean_squared_error", cv = 5))
    return(rmse)
# 岭回归
model_ridge = Ridge()
# alpha 正则化参数
alphas = [0.05, 0.1, 0.3, 1, 3, 5, 10, 15, 30, 50, 75]
# cv_ridge = [rmse_cv(Ridge(alpha = alpha)).mean()
#             for alpha in alphas]

# 保存每次交叉验证评分
rmse_list = []
for alpha in alphas:
    # 岭回归模型
    model = Ridge(alpha = alpha)
    # 计算该模型下的均方根误差
    cv_ridge = rmse_cv(model)
    rmse_list.append(cv_ridge.mean())

# 序列化交叉验证评分，评分值为均方根误差
cv_ridge = pd.Series(rmse_list, index = alphas)
# 画图plot
cv_ridge.plot(title = "Validation - Just Do It")
plt.xlabel("alpha")
plt.ylabel("rmse")

cv_ridge.min()
# lasso回归模型
model_lasso = LassoCV(alphas = [1, 0.1, 0.001, 0.0005]).fit(X_train, y)
rmse_cv(model_lasso).mean()
# 特征参数值
coef = pd.Series(model_lasso.coef_, index = X_train.columns)

print("Lasso picked " + str(sum(coef != 0)) + " variables and eliminated the other " +  str(sum(coef == 0)) + " variables")
imp_coef = pd.concat([coef.sort_values().head(10),
                      coef.sort_values().tail(10)])
matplotlib.rcParams['figure.figsize'] = (8.0, 10.0)
imp_coef.plot(kind = "barh")
plt.title("Coefficients in the Lasso Model")

#let's look at the residuals as well:
matplotlib.rcParams['figure.figsize'] = (6.0, 6.0)

preds = pd.DataFrame({"preds":model_lasso.predict(X_train), "true":y})
preds["residuals"] = preds["true"] - preds["preds"]
preds.plot(x = "preds", y = "residuals",kind = "scatter")


print("##############")