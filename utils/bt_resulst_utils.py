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
