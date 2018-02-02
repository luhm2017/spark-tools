# -*- coding: utf-8 -*-
"""
2017.12.25 luhm
"""
import os
import pandas as pd
import statsmodels.api as sm
from CalWOE import woe_trans
from logistic_reg import logistic_reg
from logistic_reg import logit_output
from model_metrics import ks_stats
from model_metrics import plot_confusion_matrix
from model_metrics import plot_roc_curve
from model_metrics import lift_lorenz
from optimal_bining_MR import _applyBinMap
from optimal_bining_MR import applyMapCats
from optimal_bining_MR import binContVar
from optimal_bining_MR import reduceCats
from creditScores import creditCards
from graph_analysis import drawPie
from graph_analysis import drawBar
from graph_analysis import drawHistogram


# if __name__ == '__main__':
# print(sys.path)
# load data

"""
数据加载
"""
path = "D:/workspace/spark-tools/src/main/data/credit-card/"
os.chdir(path)
data = pd.read_csv("default of credit card clients.csv")
df = data.copy()
y = df['label']

"""
数据勘探
"""
#drawPie(df.EDUCATION)
# drawBar(df.EDUCATION)
# drawHistogram(df.AGE)

"""
数据转换
"""
df.EDUCATION = df.EDUCATION.map({0: 'unkown', 1:1, 2:2, 3:3, 4:'unkown',5:'unkown', 6: 'unkown'})
df.MARRIAGE = df.MARRIAGE.map({0:'unkown', 1:1, 2:0, 3:'unkown'})


"""
单个变量woe转换后建立logistic模型
"""
# x_woe, woe_map, iv = _single_woe_trans(df.EDUCATION, y)
# 使用sm统计常见指标
# logit_instance, logit_model, logit_result, logit_result_0 = logistic_reg(x_woe,y)
# desc, params, evaluate, quality = logit_output(logit_instance,logit_model,logit_result,logit_result_0)

"""
对连续型变量做最优分箱处理
"""
# bining continnues var
continnues = ['LIMIT_BAL', 'AGE',
              'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3',
              'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
              'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3',
              'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6']
continnues_test = ['AGE']
# 取所有连续型变量
dc = df[continnues]

# 数据分箱，对X变量作分箱处理
def data_bining(dc, y, method):
    """
    temp function for data bining
    """
    # pandas 创建 dataFrame
    new_ds = pd.DataFrame()
    # 变量每一列
    for v in dc.columns:
        x = dc[v]
        # 连续型变量最优分箱
        bin_map = binContVar(x, y, method)
        # 根据最优分箱，转换x变量
        new_x = _applyBinMap(x, bin_map)
        new_x.name = v + "_BIN"
        new_ds = pd.concat([new_ds, new_x], axis=1)
    return new_ds

# 连续变量最优分箱
new_dc = data_bining(dc, y, method=4)

"""
类别变量做降基处理
"""
# charachter var reduce catagory
characters = ['PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6']
ds = df[characters]

bin_maps_str = dict()
for var in ds.columns:
    x = ds[var]
    single_map = reduceCats(x, y, method=4)
    bin_maps_str[x.name] = single_map

# 降基处理
new_PAY_0 = applyMapCats(df.PAY_0, bin_maps_str['PAY_0'])
new_PAY_3 = applyMapCats(df.PAY_3, bin_maps_str['PAY_3'])
new_PAY_4 = applyMapCats(df.PAY_4, bin_maps_str['PAY_4'])
new_PAY_5 = applyMapCats(df.PAY_5, bin_maps_str['PAY_5'])
new_PAY_6 = applyMapCats(df.PAY_6, bin_maps_str['PAY_6'])
new_PAY_2 = applyMapCats(df.PAY_2, bin_maps_str['PAY_2'])

# data combine
# 合并前面两步的处理数据
new_df = pd.concat([new_dc,
                    new_PAY_0,
                    new_PAY_2,
                    new_PAY_3,
                    new_PAY_4,
                    new_PAY_5,
                    new_PAY_6], axis=1)
new_df = pd.concat([new_df, df.SEX, df.EDUCATION, df.MARRIAGE], axis=1)
# WOE 转换
df_woe_X, woe_maps, iv_values = woe_trans(new_df.columns, y, new_df)
# 筛选WOE数据
varNames = df_woe_X.columns
woe_vars = varNames[varNames.str.endswith('_WOE')]
woe_df = df_woe_X[woe_vars]

# 数据划分
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


"""
生成评分卡
"""
method = 4
var_list = list(params.index)
est = params['参数估计']


# 生成评分卡
# method = 4
# # 删除参数索引名称中的 BIN 和 WOE
# params.index = [k.replace("_BIN", "") for k in params.index]
# params.index = [k.replace("_WOE", "") for k in params.index]
# paramsEst = params['参数估计']
# var_list = list(paramsEst.index)[1:]
#
# # 手动选择模型变量
# # woe_maps 参数
# woe_maps = {k.replace("_BIN", ""):v for k,v in woe_maps.items()}
# # 获取模型参数woe参数值
# woe_maps_params = {k:v for k,v in woe_maps.items() if k in var_list}
#
# # bining_maps
# numericVars = ['LIMIT_BAL', 'AGE', 'PAY_AMT1',
#                'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT5']
# bin_maps = dict()
# for v in numericVars:
#     x = df[v]
# bin_map = binContVar(x, y, method)
# bin_maps[v] = bin_map
#
# # reduce maps
# classifyVars = ['PAY_0', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6']
# red_maps = dict()
# for v in classifyVars:
#     x = df[v]
# red_map = reduceCats(x, y, method)
# red_maps[v] = red_map
#
# # 创建评分卡， 并输出生成 Excel 表格形式
# cc = creditCards(paramsEst, woe_maps_params, bin_maps, red_maps)
# # cc.to_excel("creditCard.xlsx")
