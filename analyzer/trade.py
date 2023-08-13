import backtrader as bt


class DailyTradeStats(bt.Analyzer):
    def start(self):
        self.rets['data'] = []

    def next(self):
        trade_dict = self.strategy._trades
        trade_list = []
        ts = self.strategy.datetime.date()
        for d in self.datas:
            if not trade_dict:
                continue
            if d not in trade_dict:
                continue
            trade = trade_dict[d][0][0]
            trade_info = {
                'date': ts,
                'order_book_id': d._name,
                'pnl': trade.pnl,
                'pnlcomm': trade.pnlcomm,
                'commission': trade.commission,
                'value': trade.value,
                'size': trade.size,
                'price': trade.price,
                'status': trade.status_names[trade.status],
                'ref': trade.ref,
                'dtopen': trade.open_datetime(),
                'dtclose': trade.close_datetime() if trade.dtclose >= 1 else None,

            }
            trade_list.append(trade_info)
        self.rets['data'].extend(trade_list)

    def result(self):
        import pandas as pd
        df_analysis = pd.DataFrame(self.rets['data'])
        groups = df_analysis.groupby(by=['ref'])
        res = []
        for _, group in groups:
            mask = (group['date'] == group['dtclose']) | (group['status'] == 'Open')
            group = group[mask].copy()
            group['pnl_change'] = group['pnl'] - group['pnl'].shift(1)
            group['pnlcomm_change'] = group['pnlcomm'] - group['pnlcomm'].shift(1)
            res.append(group)
        return pd.concat(res, axis=0)
