# -*- coding: utf-8 -*-

import pandas as pd


### balancesheet


def get_balancesheet_data(stock_code):
    df = pd.read_pickle("./balancesheet/%s.pkl" % stock_code)
    return dropduplicate(df)


def get_income_data(stock_code):
    df = pd.read_pickle("./income/%s.pkl" % stock_code)
    return dropduplicate(df)


def get_cashflow_data(stock_code):
    df = pd.read_pickle("./cashflow/%s.pkl" % stock_code)
    return dropduplicate(df)


def get_financial_index_data(stock_code):
    df = pd.read_pickle("./financial_index/%s.pkl" % stock_code)
    return dropduplicate(df)


def dropduplicate(fincanial_df):
    fincanial_df.sort_values(by=['end_date', 'ann_date'], inplace=True)
    fincanial_df.drop_duplicates(subset=['end_date'], keep='first', inplace=True)
    return fincanial_df

def get_tgt_series(df, index_name):
    if index_name in df.columns.values:
        return df[index_name]
    else:
        return df['%s_x' % index_name]

def  get_last_year_index(financial_index, method='period'):
    """
    获取去年一年的指标,根据method 确定计算方法
    如果method 为period，针对利润表，现金流量表, 如果为status,针对资产负载表
    :param financial_index:
    :return:
    """
    financial_info_dict = financial_index.to_dict()
    rpt_end_dates = financial_info_dict.keys()
    rpt_end_dates.sort()
    def cal_last_year_index(end_date):
        year = end_date[:4]
        month = end_date[4: 6]
        day = end_date[6:]
        last_year_q = "%s%s%s" % (int(year)-1, month, day)
        last_year_end = "%s1231" % (int(year)-1)
        last_year_q_index = financial_info_dict.get(last_year_q, None)
        last_year_end_index = financial_info_dict.get(last_year_end, None)
        this_q_index = financial_info_dict.get(end_date, None)
        if method == 'period':
            if last_year_q_index and last_year_end_index and this_q_index:
                last_year_index = last_year_end_index-last_year_q_index+this_q_index
            else:
                last_year_index = None
        else:
            if last_year_q_index and this_q_index:
                last_year_index = (last_year_q_index+this_q_index)/2.0
            else:
                last_year_index = None
        return last_year_index
    last_year_financial_index = pd.Series(financial_index.index.map(cal_last_year_index).values, index=financial_index.index)
    return last_year_financial_index


def generate_quality_factor(stock_code):
    balancesheet_df = get_balancesheet_data(stock_code)

    income_df = get_income_data(stock_code)

    cashflow_df = get_cashflow_data(stock_code)

    financial_index_df = get_financial_index_data(stock_code)
    print balancesheet_df['goodwill']
    # print income_df.head(1).T
    financial_performance_df = pd.merge(income_df, balancesheet_df, on='end_date', how='inner')
    financial_performance_df = pd.merge(financial_performance_df, cashflow_df, on='end_date', how='inner')
    financial_performance_df = pd.merge(financial_performance_df, financial_index_df, on='end_date', how='inner')

    financial_performance_df.set_index('end_date', inplace=True)
    financial_performance_df['n_income_attr_p_last_year'] = get_last_year_index(financial_performance_df['n_income_attr_p'])
    financial_performance_df['ebit_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'ebit'))
    financial_performance_df['profit_dedt_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'profit_dedt'))
    financial_performance_df['gross_margin_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'gross_margin'))
    financial_performance_df['revenue_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'revenue'))
    financial_performance_df['total_hldr_eqy_exc_min_int_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'total_hldr_eqy_exc_min_int'), method='status')
    financial_performance_df['total_cur_assets_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'total_cur_assets'), method='status')
    financial_performance_df['total_cur_liab_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'total_cur_liab'), method='status')
    financial_performance_df['inventories_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'inventories'), method='status')
    financial_performance_df['total_assets_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'total_assets'), method='status')
    financial_performance_df['goodwill_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'goodwill'), method='status')

    financial_performance_df['n_cashflow_act_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'n_cashflow_act'))
    financial_performance_df['accounts_receiv_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'accounts_receiv'), method='status')
    financial_performance_df['total_liab_last_year'] = get_last_year_index(
        get_tgt_series(financial_performance_df, 'total_liab'), method='status')


    ### 净利率 ###
    financial_performance_df['net_profit_ratio'] = \
        financial_performance_df.apply(lambda x: x['n_income_attr_p_last_year']/x['revenue_last_year']
        if x['n_income_attr_p_last_year'] and x['revenue_last_year'] is not None else None, axis=1)
    ### 净利率 剔除非经常性损益 ###
    financial_performance_df['net_profit_ratio_v2'] = \
        financial_performance_df.apply(lambda x: x['profit_dedt_last_year']/x['revenue_last_year']
        if x['profit_dedt_last_year'] and x['revenue_last_year'] is not None else None, axis=1)
    ###毛利率####
    financial_performance_df['gross_profit_rate'] = \
        financial_performance_df.apply(lambda x: x['gross_margin_last_year']/x['revenue_last_year']
        if x['gross_margin_last_year'] and x['revenue_last_year'] is not None else None, axis=1)
    ###净资产收益率=净利润/股东权益###
    financial_performance_df['roe'] = financial_performance_df.\
        apply(lambda x: x['n_income_attr_p_last_year']/x['total_hldr_eqy_exc_min_int_last_year']
    if x['total_hldr_eqy_exc_min_int'] and x['n_income_attr_p_last_year'] is not None else None, axis=1)
    ###净资产收益率=净利润/股东权益,剔除非经常性损益###
    financial_performance_df['roe_v2'] = financial_performance_df.\
        apply(lambda x: x['profit_dedt_last_year']/x['total_hldr_eqy_exc_min_int_last_year']
    if x['total_hldr_eqy_exc_min_int'] and x['profit_dedt_last_year'] is not None else None, axis=1)


    ###流动比率＝流动资产合计/流动负债合计###
    financial_performance_df['currentratio'] = financial_performance_df.\
        apply(lambda x: x['total_cur_assets_last_year']/x['total_cur_liab_last_year']
    if x['total_cur_assets_last_year'] and x['total_cur_liab_last_year'] is not None else None, axis=1)

    ### 速动比率=速动资产/流动负债 ###
    financial_performance_df['quickratio'] = financial_performance_df.\
        apply(lambda x: (x['total_cur_assets_last_year']-x['inventories_last_year'])/x['total_cur_liab_last_year']
    if x['total_cur_assets_last_year'] and x['inventories_last_year'] is not None and
       x['total_cur_liab_last_year'] is not None else None, axis=1)


    ### 商誉/总资产####
    financial_performance_df['goodwillratio'] = financial_performance_df.\
        apply(lambda x: x['goodwill_last_year']/x['total_assets_last_year']
    if x['goodwill_last_year'] is not None and x['total_assets_last_year'] is not None else 0, axis=1)

    ###股东权益比例＝股东权益总额/资产总额###
    financial_performance_df['sheqratio'] = financial_performance_df.\
        apply(lambda x: x['total_hldr_eqy_exc_min_int_last_year']/x['total_assets_last_year']
    if x['total_hldr_eqy_exc_min_int_last_year'] and x['total_assets_last_year'] is not None else None, axis=1)

    ###经营现金净流量对销售收入比率=经营活动现金净流量/主营业务收入###
    financial_performance_df['cf_sales'] = financial_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['revenue_last_year'] if x['revenue_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ###资产的经营现金流量回报率=经营活动现金净流量/总资产###
    financial_performance_df['rateofreturn'] = financial_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_assets_last_year'] if x['total_assets_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)

    ####经营现金净流量与净利润的比率#####
    financial_performance_df['cf_nm'] = financial_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['n_income_attr_p_last_year'] if x['n_income_attr_p_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ####经营现金净流量对负债比率=经营活动产生的现金净流量/负债总额###

    financial_performance_df['cf_liabilities'] = financial_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_liab_last_year'] if x['total_liab_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)
    ####经营现金净流量对负债比率=经营活动产生的现金净流量/流动负债###
    financial_performance_df['cashflowratio'] = financial_performance_df.\
        apply(lambda x: x['n_cashflow_act_last_year']/x['total_cur_liab_last_year'] if x['total_cur_liab_last_year'] and x['n_cashflow_act_last_year'] is not None else None, axis=1)



    print financial_performance_df[['net_profit_ratio', 'net_profit_ratio_v2', 'gross_profit_rate', 'roe',
                                    'roe_v2', 'currentratio', 'quickratio',  'sheqratio',
                                    'goodwillratio', 'sheqratio', 'cf_sales', 'rateofreturn', 'cf_nm',
                                    'cf_liabilities', 'cashflowratio']].tail(4).T



generate_quality_factor("000858.SZ")

