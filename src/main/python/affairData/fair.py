from __future__ import print_function
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.formula.api import logit, probit, poisson, ols
from optimal_bining_MR import _applyBinMap
from optimal_bining_MR import applyMapCats
from optimal_bining_MR import binContVar
from optimal_bining_MR import reduceCats
from CalWOE import woe_trans
import pandas as pd
from logistic_reg import logistic_reg
from logistic_reg import logit_output
from model_metrics import ks_stats
from model_metrics import plot_confusion_matrix
from model_metrics import plot_roc_curve

print(sm.datasets.fair.SOURCE)
print(sm.datasets.fair.NOTE)

# 加载数据
dta = sm.datasets.fair.load_pandas().data
# label
dta['affair'] = (dta['affairs'] > 0).astype(float)
y = dta['affair']
# 数据勘探
# print(dta.describe())


"""
类别变量做降基处理
"""
characters = ['occupation', 'educ', 'occupation_husb', 'rate_marriage', 'age', 'yrs_married','children','religious']
ds = dta[characters]
# 降基处理
bin_maps_str = dict()
for var in ds.columns:
    x = ds[var]
    single_map = reduceCats(x, y, method=4)
    bin_maps_str[x.name] = single_map

# 降基处理
new_occupation = applyMapCats(dta.occupation, bin_maps_str['occupation'])
new_educ = applyMapCats(dta.educ, bin_maps_str['educ'])
new_occupation_husb = applyMapCats(dta.occupation_husb, bin_maps_str['occupation_husb'])
new_rate_marriage = applyMapCats(dta.rate_marriage, bin_maps_str['rate_marriage'])
new_age = applyMapCats(dta.age, bin_maps_str['age'])
new_yrs_married = applyMapCats(dta.yrs_married, bin_maps_str['yrs_married'])
new_children = applyMapCats(dta.children, bin_maps_str['children'])
new_religious = applyMapCats(dta.religious, bin_maps_str['religious'])

# 合并数据
new_df = pd.concat([new_occupation,
                    new_educ,
                    new_occupation_husb,
                    new_rate_marriage,
                    new_age,
                    new_yrs_married,
                    new_children,
                    new_religious], axis=1)
# WOE转换
df_woe_X, woe_maps, iv_values = woe_trans(ds.columns, y, ds)
# 筛选WOE数据
varNames = df_woe_X.columns
woe_vars = varNames[varNames.str.endswith('_WOE')]
woe_df = df_woe_X[woe_vars]

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(woe_df, y, test_size=0.33)

"""
模型开发
"""
logit_instance, logit_model, logit_result, logit_result_0 = logistic_reg(X_train,
                                                                         y_train,
                                                                         stepwise='BS') # BS

desc, params, evaluate, quality = logit_output(logit_instance,
                                               logit_model,
                                               logit_result,
                                               logit_result_0)

"""
模型评价
"""
# prob_y_train = logit_result.predict()
# 测试数据集
X_test_metric = sm.add_constant(X_test[params.index[1:]])
# 测试数据结果
prob_y_test = logit_result.predict(X_test_metric)
# 结果集按
label_pred_test = pd.np.where(prob_y_test > 0.5, 1, 0)

# ROC 曲线
plot_roc_curve(prob_y_test, y_test)
# KS表&KS曲线
ks_stattable, _ = ks_stats(prob_y_test, y_test)
# 提升图&lorenz曲线
# lift_lorenz(prob_y_test, y_test)
# 构造混淆矩阵
plot_confusion_matrix(y_test, label_pred_test, labels=[0,1])