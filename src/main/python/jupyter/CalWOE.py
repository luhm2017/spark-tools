# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 20:33:32 2017

@author: Hank Kuang
@title: WOE转换和信息值(info value)计算
"""

import numpy as np
import math
from sklearn.utils.multiclass import type_of_target
import pandas as pd


# 计算单变量的woe值和信息值
def woe_single_x(x, y, event=1, EPS=1e-7):
    """
    calculate woe and information for a single feature
    -----------------------------------------------
    Param 
    x: 1-D pandas dataframe starnds for single feature
    y: pandas Series contains binary variable
    event: value of binary stands for the event to predict
    -----------------------------------------------
    Return
    dictionary contains woe values for categories of this feature
    information value of this feature
    """
    
    _check_target_binary(y)

    event_total, non_event_total = _count_binary(y, event=event)
        
    x_labels = x.unique()
    #x_labels = np.unique(x)
    woe_dict = {}
    iv = 0
    for x1 in x_labels:
            
        y1 = y[np.where(x == x1)[0]]
        event_count, non_event_count = _count_binary(y1, event=event)
        rate_event = 1.0 * event_count / event_total#
        rate_non_event = 1.0 * non_event_count / non_event_total#
        if rate_event == 0:#
            rate_event = EPS
        elif rate_non_event == 0:#
            rate_non_event = EPS
        else:
            pass
        woe1 = math.log(rate_event / rate_non_event)#
        woe_dict[x1] = woe1
        iv += (rate_event - rate_non_event) * woe1#
    return woe_dict, iv
    
# 统计分类结果值
def _count_binary(a, event=1):
    """
    calculate the cross table of a
    ------------------------------
    Params
    a: pandas Series contains binary variable
    event: treate as 1, others as 0
    ------------------------------
    Return
    event_count: numbers of event=1
    non_event_count: numbers of event!=1
    """
    event_count = (a == event).sum()
    non_event_count = a.shape[-1] - event_count
    return event_count, non_event_count


# 检验分类值
def _check_target_binary(y):
    """
    check if the target variable is binary
    ------------------------------
    Param
    y:exog variable, pandas Series contains binary variable
    ------------------------------
    Return
    if y is not binary, raise a error   
    """
    y_type = type_of_target(y)
    if y_type not in ['binary']:
        raise ValueError('目标变量必须是二元的！')
        
# 数据woe转换
def _single_woe_trans(x, y):
    """
    single var's woe trans
    ---------------------------------------
    Param
    x: single exog, pandas series
    y: endog, pandas series
    ---------------------------------------
    Return
    x_woe_trans: woe trans by x
    woe_map: map for woe trans
    info_value: infor value of x
    """
    #cal_woe = WOE()
    # woe_map 对应每个段的woe值
    woe_map, info_value = woe_single_x(x, y)
    # map函数返回新的list
    x_woe_trans = x.map(woe_map)
    # 每个分段添加 别名
    x_woe_trans.name = x.name + "_WOE"
    return x_woe_trans, woe_map, info_value

# woe转换
def woe_trans(varnames, y, df):
    """
    WOE translate for multiple vars
    ---------------------------------------
    Param
    varnames: list
    y:  pandas series, target variable
    df: pandas dataframe, endogenous vars
    ---------------------------------------
    Return
    df: pandas dataframe, trans results
    woe_maps: dict, key is varname, value is woe
    iv_values: dict, key is varname, value is info value
    """
    iv_values = {}
    woe_maps = {}
    for var in varnames:
        x = df[var]
        x_woe_trans, woe_map, info_value = _single_woe_trans(x, y)
        df = pd.concat([df, x_woe_trans], axis=1)
        woe_maps[var] = woe_map
        iv_values[var] = info_value
    
    return df, woe_maps, iv_values

