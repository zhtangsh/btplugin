import numpy as np
import pandas as pd
import datetime

FREQ_ONEYEAR_MAP = {
    'D': 252,
    '1D': 252,
    "W": 50,
    '1W': 50,
    'M': 12,
    'HD': 2,
    'Y': 1
}
FREQ_GROUPER_MAP = {
    'D': 'D',
    '1D': 'D',
    "W": 'W-FRI',
    '1W': 'W-FRI',
    'M': 'MS',
    'Y': 'Y'
}
DAYS_IN_PERIOD = {
    'D': 1,
    '1D': 1,
    "W": 5,
    '1W': 5,
    'M': 21,
    'HD': 126,
    'Y': 252
}
FREQ_TIME_FORMAT_REF = {
    'D': '%Y-%m-%d',
    '1D': '%Y-%m-%d',
    "W": '%YW%U',
    '1W': '%YW%U',
    'M': '%Y-%m',
    'Y': '%Y'
}


def get_netvalue_analysis(netvalue, freq, rf) -> pd.Series:
    """
    由净值序列进行指标统计
    :param netvalue: pd.Series
    :param freq: 收益率频率
    :param rf: 无风险利率
    :return:pd.Series
    """
    freq = freq.upper()

    if len(netvalue) == 0 or netvalue is None:
        return pd.Series({
            '累计收益率': np.nan,
            '年化收益率': np.nan,
            '年化波动率': np.nan,
            '最大回撤率': np.nan,
            '胜率(' + freq + ')': np.nan,
            '盈亏比': np.nan,
            '夏普比率': np.nan,
            'Calmar比': np.nan,
        }, name='analysis')
    if freq not in FREQ_ONEYEAR_MAP:
        raise ValueError('get_netvalue_analysis -- Not Right freq : ', freq)
    oneyear = FREQ_ONEYEAR_MAP[freq]
    # 交易次数
    tradeslen = netvalue.shape[0]
    # 收益率序列
    tmp = netvalue.shift()
    tmp[0] = 1
    returns = netvalue / tmp - 1
    # 累计收益率
    totalreturn = netvalue.iloc[-1] - 1
    # 年化收益率
    return_yr = (1 + totalreturn) ** (oneyear / tradeslen) - 1
    # 年化波动率
    volatility_yr = np.std(returns, ddof=0) * np.sqrt(oneyear)
    if volatility_yr == 0.0:
        return pd.Series({
            '累计收益率': np.nan,
            '年化收益率': np.nan,
            '年化波动率': np.nan,
            '最大回撤率': np.nan,
            '胜率(' + freq + ')': np.nan,
            '盈亏比': np.nan,
            '夏普比率': np.nan,
            'Calmar比': np.nan,
        }, name='analysis')
    else:
        # 夏普比率
        sharpe = (return_yr - rf) / volatility_yr
        # 回撤
        drawdowns = get_maxdrawdown(netvalue)
        # 最大回撤
        maxdrawdown = min(drawdowns)
        # 收益风险比
        if maxdrawdown == 0:
            profit_risk_ratio = np.inf
        else:
            profit_risk_ratio = return_yr / np.abs(maxdrawdown)
        # 盈利次数
        win_count = (returns > 0).sum()
        # 亏损次数
        lose_count = (returns < 0).sum()
        # 胜率
        win_rate = win_count / (win_count + lose_count)
        # 盈亏比
        p_over_l = returns[returns > 0].mean() / np.abs(returns[returns < 0].mean())
    return pd.Series({
        '累计收益率': totalreturn,
        '年化收益率': return_yr,
        '年化波动率': volatility_yr,
        '最大回撤率': maxdrawdown,
        '胜率(' + freq + ')': win_rate,
        '盈亏比': p_over_l,
        '夏普比率': sharpe,
        'Calmar比': profit_risk_ratio,
    }, name='analysis')


def get_maxdrawdown(netvalue) -> pd.Series:
    """
    最大回撤率计算
    :param netvalue: pd.Series
    :return:
    """
    maxdrawdowns = pd.Series(index=netvalue.index, dtype='float64')
    for i in np.arange(len(netvalue.index)):
        highpoint = netvalue.iloc[0:(i + 1)].max()
        if highpoint == netvalue.iloc[i]:
            maxdrawdowns.iloc[i] = 0
        else:
            maxdrawdowns.iloc[i] = netvalue.iloc[i] / highpoint - 1

    return maxdrawdowns


def average_turnover(position_df: pd.DataFrame, transaction_df: pd.DataFrame, freq: str = 'Y') -> float:
    """
    :param position_df: 仓位表
    :param transaction_df: 交易表
    :param freq: 计算换手率的基准频率
    :return: 平均换手率
    计算平均换手率
    在基准频率(周度/阅读/年度)上，计算
    1. 总交易量 total_value = Sum(abs(transaction.value))
    2. 期末持仓总量 position_value = Last(position)
    3. factor特殊处理，年化换手率需添加年化参数，值为回测中交易日/255
    4. 计算该频率上的换手率, total_value/position_value*factor
    返回所有换手率的平均值
    """
    if freq not in DAYS_IN_PERIOD or freq not in FREQ_GROUPER_MAP:
        raise ValueError('average_turnover -- Not Right freq : ', freq)
    grouper_key = FREQ_GROUPER_MAP[freq]
    position_df['sum'] = position_df.sum(axis=1) - position_df['cash']
    transaction_df = transaction_df.reset_index()
    position_df = position_df.reset_index()
    transaction_info = transaction_df.groupby(pd.Grouper(key='date', freq=grouper_key)).agg(
        total_value=pd.NamedAgg(column='value', aggfunc=lambda x: x.abs().sum()),
    )
    position_info = position_df.groupby(pd.Grouper(key='date', freq=grouper_key)).agg(
        position_value=pd.NamedAgg(column='sum', aggfunc='last'),
        total_days=pd.NamedAgg(column='date', aggfunc='nunique'),
        start_date=pd.NamedAgg(column='date', aggfunc='min'),
        end_date=pd.NamedAgg(column='date', aggfunc='max'),
    )
    merged_df = position_info.join(transaction_info)
    merged_df = merged_df[~merged_df['position_value'].isna()].copy()
    merged_df['turnover_rate'] = merged_df['total_value'] / merged_df['position_value']
    return merged_df['turnover_rate'].mean() / DAYS_IN_PERIOD[freq] * 252


def future_average_turnover(
        position_df: pd.DataFrame,
        transaction_df: pd.DataFrame,
        freq: str = 'Y',
        mult_dict={}
) -> float:
    """
    :param transaction_df: 交易表
    :return: 平均换手率
    :param freq: 计算换手率的基准频率
    计算期货平均换手率
    基于交易数据，计算平均换手率
    1. 按年分组
    2. 每年内计算平均市值
    3. 每年内计算总成交额
    2. 成交金量/平均市值得日度换手率
    3. 计算平均换手率
    """
    if freq not in DAYS_IN_PERIOD or freq not in FREQ_GROUPER_MAP:
        raise ValueError('average_turnover -- Not Right freq : ', freq)
    grouper_key = FREQ_GROUPER_MAP[freq]
    column_mask = [c for c in position_df.columns if 'market_value' in c]
    position_df = position_df[column_mask].copy()
    position_df['sum'] = position_df.apply(lambda x: x.abs().sum(), axis=1)
    position_df['sum'] = position_df['sum'].replace(0.0, np.nan)
    transaction_df['value_with_mult'] = transaction_df.apply(lambda x: x['value'] * mult_dict.get(x['symbol'], 1.), axis=1)
    # 交易按日做sum，持仓按日做平均
    position_df = position_df.groupby(pd.Grouper(freq='D'))[['sum']].mean()
    transaction_df = transaction_df.groupby(pd.Grouper(freq='D')).agg(
        total_value=pd.NamedAgg(column='value_with_mult', aggfunc=lambda x: x.abs().sum()),
    )

    # 按年计算平均换手率
    position_df = position_df.reset_index()
    transaction_df = transaction_df.reset_index()
    transaction_info = transaction_df.groupby(pd.Grouper(key='date',freq=grouper_key)).agg(
        total_value=pd.NamedAgg(column='total_value', aggfunc=lambda x: x.abs().sum()),
    )
    position_info = position_df.groupby(pd.Grouper(key='date',freq=grouper_key)).agg(
        mean_position_value=pd.NamedAgg(column='sum', aggfunc='mean'),
        total_days=pd.NamedAgg(column='date', aggfunc='nunique'),
        start_date=pd.NamedAgg(column='date', aggfunc='min'),
        end_date=pd.NamedAgg(column='date', aggfunc='max'),
    )
    merged_df = position_info.join(transaction_info)
    merged_df = merged_df[~merged_df['mean_position_value'].isna()].copy()
    merged_df['turnover_rate'] = merged_df['total_value'] / merged_df['mean_position_value'] \
                                 / merged_df['total_days'] * 252
    return merged_df['turnover_rate'].mean()


def get_yearly_analysis(netvalue, freq, rf=0) -> pd.DataFrame:
    """
    计算年化收益率
    :param netvalue: pd.Series
    :param freq: 收益率频率
    :param rf: 无风险利率
    :return:pd.Series
    """
    df = pd.DataFrame(netvalue.rename('npv'))
    df['dt'] = df.index
    df['year'] = df['dt'].apply(lambda x: str(x.year))
    years = df['year'].unique()
    ret = pd.DataFrame()
    init_npv = 1
    for y in years:
        npv = df[df['year'] == y]['npv']
        year_npv = npv / init_npv
        init_npv = npv.iloc[-1]
        r = get_netvalue_analysis(year_npv, freq=freq, rf=rf)
        ret[y] = r
    return ret
