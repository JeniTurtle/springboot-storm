import pywt
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import r2_score
import storm
import collections
import warnings
import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings("ignore")

"""小波分解"""

class WaveDecompositionBolt(storm.BasicBolt):
  def rec_coeff(self, data):
    #分解使用的小波
    w = pywt.Wavelet("db5")
    #数据长度
    data_length = len(data)
    #进行分解得到系数
    coeffs = pywt.wavedec(data,'db5',level=5)
    #分解层数
    coeff_length = len(coeffs)
    rec_data = []
    for i in range(1,coeff_length):
        coef = coeffs[:-i] + [None] * i
        rec_coef = pywt.waverec(coef,w)
        rec_coef_interp = np.interp(x = np.arange(0, data_length), xp = np.linspace(0, data_length, len(rec_coef)), fp = rec_coef)
        #补全后的每层系数追加到数组l
        rec_data.append(rec_coef_interp)
    return rec_data

  """回归分析"""
  def analize_regression(self, rec_data):
    #近似系数求趋势
    #近似系数
    approx=rec_data[-1]
    x=[i for i in range(len(approx))]
    #回归分析
    curve_fit = np.polyfit(x,approx,2)
    #最小二乘法多项式拟合曲线
    fit_value = np.poly1d(curve_fit)
    predicted = fit_value(x)
    #coefficient_of_dermination = r2_score(approx,predicted)
    return predicted

  def order_dict(self, dict_data):
    ordered_dict = collections.OrderedDict()
    ordered_keys = sorted(dict_data.keys())
    for key in ordered_keys:
        ordered_dict[key] = dict_data[key]
    return ordered_dict

  def process(self, tup):
    dict_data = tup.values[0]
    sensor_type = tup.values[1]
    first_dispatch_time = tup.values[2]

    ordered_dict = self.order_dict(dict_data)

    values = list(dict_data.values())
    rec_data = self.rec_coeff(values)

    rec_list = []
    for ret_item in rec_data:
        new_dict = dict(zip(dict_data.keys(), map(lambda x: round(x, 4), ret_item)))
        rec_list.append(new_dict)

    regression = self.analize_regression(rec_data)
    regression_data = dict(zip(dict_data.keys(), map(lambda x: round(x, 4), regression)))

    storm.emit([rec_list, regression_data, sensor_type, first_dispatch_time])

WaveDecompositionBolt().run()