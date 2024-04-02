import pandas as pd


def build_position_value(ordered_list) -> pd.DataFrame:
    head = ordered_list['Datetime']
    res = []
    for k, v in ordered_list.items():
        if k == 'Datetime':
            continue
        data = {'date': k}
        for i in range(len(head)):
            order_book_id = head[i]
            data[order_book_id] = v[i]
        res.append(data)
    if not res:
        return pd.DataFrame()
    df = pd.DataFrame(res)
    df['date'] = pd.to_datetime(df['date'])
    df['date'].dt.tz_localize(None)
    return df.set_index('date')


def build_market_data(ordered_list) -> pd.DataFrame:
    head = ordered_list['Datetime']
    res = []
    for k, v in ordered_list.items():
        if k == 'Datetime':
            continue
        data = {'date': k}
        for i in range(len(head)):
            order_book_id = head[i]
            data[order_book_id] = v[i]
        res.append(data)
    if not res:
        return pd.DataFrame()
    df = pd.DataFrame(res)
    df['date'] = pd.to_datetime(df['date'])
    df['date'].dt.tz_localize(None)
    return df.set_index('date')


def build_transaction(ordered_list) -> pd.DataFrame:
    head = ordered_list['date'][0]
    res = []
    for k, transaction_list in ordered_list.items():
        if k == 'date':
            continue
        for transaction in transaction_list:
            data = {'date': k}
            for i in range(len(head)):
                data[head[i]] = transaction[i]
            res.append(data)
    if not res:
        return pd.DataFrame()
    df = pd.DataFrame(res)
    df['date'] = pd.to_datetime(df['date'])
    df['date'].dt.tz_localize(None)
    return df.set_index('date')


def patch_future_position(
        p_df: pd.DataFrame,
        t_df: pd.DataFrame,
        market_data_df: pd.DataFrame,
        mult_dict) -> pd.DataFrame:
    t_df['volume'] = t_df.groupby(['symbol'])['amount'].cumsum()
    holding_df = t_df[['symbol', 'volume']].copy().reset_index()
    holding_pivot_df = pd.pivot_table(holding_df, index=['date'], values=['volume'], columns=['symbol'], dropna=False)
    holding_pivot_df.columns = holding_pivot_df.columns.droplevel()
    market_value_df = holding_pivot_df * market_data_df
    market_value_df = market_value_df.copy()
    for c in market_value_df.columns:
        if c not in mult_dict:
            mult = 1
        else:
            mult = mult_dict[c]
        market_value_df[c] = market_value_df[c] * mult
    margin_column_map = {c: f"{c}_margin" for c in p_df.columns if c != 'cash'}
    column_map = {c: f"{c}_market_value" for c in market_value_df.columns}
    p_df = p_df.rename(columns=margin_column_map)
    market_value_df = market_value_df.rename(columns=column_map)
    return pd.concat([market_value_df, p_df], axis=1)


def build_trade_history(df_in) -> pd.DataFrame:
    df_in['unrealized_pnl'] = (df_in['close'] - df_in['price']) * df_in['size']
    groups = df_in.groupby(by=['ref'])
    res = []
    for _, group in groups:
        mask = (group['date'] == group['dtclose']) | (group['status'] == 'Open')
        group = group[mask].copy()
        group['overall_pnl'] = group['pnl'] + group['unrealized_pnl']
        group['overall_pnlcomm'] = group['overall_pnl'] - group['commission']
        group['overall_pnl_change'] = group['overall_pnl'] - group['overall_pnl'].shift(1)
        group['overall_pnlcomm_change'] = group['overall_pnlcomm'] - group['overall_pnlcomm'].shift(1)
        res.append(group)
    return pd.concat(res, axis=0)
