# -*- coding: utf-8 -*-

from datetime import datetime
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

def show_figure(df, y1_columns, y2_columns, start_date, end_date, time_graduation='month'):
    df = df[df.index.map(lambda x: x>=start_date and x<=end_date)]
    dates = df.index.values
    xs = [datetime.strptime(d, '%Y%m%d').date() for d in dates]
    # ys = data_s.values
    # 配置横坐标
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y%m%d'))
    assert time_graduation in ['day', 'month', 'year']
    if time_graduation == 'day':
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    elif time_graduation == 'month':
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    else:
        plt.gca().xaxis.set_major_locator(mdates.YearLocator())
    # Plot
    # for column_name in columns:
    #     plt.plot(xs, df[column_name].values)
    # plt.legend(columns)
    plt.gcf().autofmt_xdate()  # 自动旋转日期标记
    plt.show()
    fig = plt.figure()

    ax1 = fig.add_subplot(111)
    for y1_column_name in y1_columns:
        ax1.plot(xs, df[y1_column_name])
    ax1.legend(y1_columns)
    # ax1.set_ylabel('Y values for exp(-x)')
    # ax1.set_title("Double Y axis")

    ax2 = ax1.twinx()  # this is the important function
    if len(y2_columns):
        for y2_column_name in y2_columns:
            ax2.plot(xs, df[y2_column_name], '-')
    plt.show()

def get_fit_error_std(y):
    lr_clf = LinearRegression()
    y = np.array(y)
    x = np.arange(len(y)).reshape(-1, 1)
    lr_clf.fit(x, y)
    y_pred = lr_clf.predict(x)
    return np.std(y_pred-y)


def get_mv(df, window_size):
    df.sort_index(inplace=True)
    df['pivot'] = df.apply(lambda x: (x['open']+x['close']+x['high']+x['low'])/4, axis=1)
    df["ma_%s" % window_size] = df['pivot'].rolling(window=window_size).mean()
    ma_std = df['pivot'].rolling(window=window_size).std()
    df['ma_%s_up' % window_size] = df["ma_%s" % window_size] + 2*ma_std
    df['ma_%s_down' % window_size] = df["ma_%s" % window_size] - 2*ma_std
    ma_std_fit = df['pivot'].rolling(window=window_size).apply(lambda x: get_fit_error_std(x))
    # print df['m20_std_fit']
    df['ma_%s_up_fit' % window_size] = df["ma_%s" % window_size] + 2*ma_std_fit
    df['ma_%s_down_fit' % window_size] = df["ma_%s" % window_size] - 2*ma_std_fit
    return df

df = pd.read_pickle("./market_kline/000001.SZ.pkl")
df = get_mv(df, 60)
# df = get_mv(df, 5)
df['volume_ratio_std'] = df['volume_ratio'].map(lambda x: int(x*10)/10.0)
show_figure(df, ['pivot', 'ma_60', 'ma_60_up_fit', 'ma_60_down_fit', 'ma_60_up', 'ma_60_down'], [], '20150101', '20181001', 'month')