# -*- coding: utf-8 -*-
"""
2017.12.25 luhm
"""
import os
from graph_analysis import drawPie
from graph_analysis import drawBar
from graph_analysis import valueCounts
from graph_analysis import drawHistogram
from CalWOE import woe_single_x
from logistic_reg import logistic_reg
from logistic_reg import logit_output
import pandas as pd

from src.main.python.creditCardScore.CalWOE import _single_woe_trans

if __name__ == '__main__':
    # print(sys.path)
    # load data
    path = "D:/workspace/spark-tools/src/main/data/credit-card/"
    os.chdir(path)
    data = pd.read_csv("default of credit card clients.csv")
    df = data.copy()
    # target
    #y = df['default payment next month']
    y = df['label']
    #df = df.drop([0])

    """
    数据勘探
    """
    #print(df.EDUCATION.unique())
    #print(df.MARRIAGE.unique())

    """
    数据转换
    """
    df.EDUCATION = df.EDUCATION.map({0: 'unkown', 1:1, 2:2, 3:3, 4:'unkown',5:'unkown', 6: 'unkown'})
    df.MARRIAGE = df.MARRIAGE.map({0:'unkown', 1:1, 2:0, 3:'unkown'})

    """
    单个变量woe转换后建立logistic模型
    """
    x_woe, woe_map, iv = _single_woe_trans(df.EDUCATION, y)

    logit_instance, logit_model, logit_result, logit_result_0 = logistic_reg(x_woe,
                                                                             y)
    desc, params, evaluate, quality = logit_output(logit_instance,
                                                   logit_model,
                                                   logit_result,
                                                   logit_result_0)

    """
    数据勘探分析
    """
    # drawPie(df.EDUCATION)
    # drawBar(df.EDUCATION)
    # print(valueCounts(df.EDUCATION))
    # drawHistogram(df.AGE)

    """
    计算特征变量信息值
    """
    woe_dict, iv = woe_single_x(df.MARRIAGE, y)
    print(woe_dict, iv)

    """
    最优分箱
    """

