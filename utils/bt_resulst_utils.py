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
    return df.set_index('date')
