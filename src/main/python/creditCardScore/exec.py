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
import pandas as pd

if __name__ == '__main__':
    # print(sys.path)
    # load data
    path = "E:/WORKSPACE/spark-tools/src/main/data/credit-card/"
    os.chdir(path)
    data = pd.read_csv("default of credit card clients.csv")
    df = data.copy()
    # target
    y = df['default payment next month']
    #df = df.drop([0])
    # df = df

    """
    数据勘探
    """
    #print(df.EDUCATION.unique())
    #print(df.MARRIAGE.unique())

    """
    数据预处理
    """
    df.EDUCATION = df.EDUCATION.map({0: 'unkown', 1:1, 2:2, 3:3, 4:'unkown',5:'unkown', 6: 'unkown'})
    df.MARRIAGE = df.MARRIAGE.map({0:'unkown', 1:1, 2:0, 3:'unkown'})

    """
    数据勘探分析
    """
    #drawPie(df.EDUCATION)
    #drawBar(df.EDUCATION)
    #print(valueCounts(df.EDUCATION))
    #drawHistogram(df.AGE)

    """
    计算特征变量信息值
    """
    woe_dict, iv = woe_single_x(df.MARRIAGE, y)
    print(woe_dict, iv)

    """
    最优分箱
    """

