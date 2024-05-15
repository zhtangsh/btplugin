import datetime
import logging

import backtrader as bt
import pandas as pd
from btplugin.utils import analysis_util, bt_resulst_utils


class DailyTradeStats(bt.Analyzer):
    params = (
        ('contribution_freq', 'Y'),  # 贡献分析的频率
        ('k_largest', '10'),  # top 票
    )

    def start(self):
        self.rets['data'] = {}

    def next(self):
        trade_dict = self.strategy._trades
        trade_list = []
        ts = self.strategy.datetime.datetime()
        t_date = ts.date()
        for d in self.datas:
            if not trade_dict:
                continue
            if d not in trade_dict:
                continue
            for trade in trade_dict[d][0]:
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
                    'open': d.open[0],
                    'close': d.close[0],

                }
                trade_list.append(trade_info)
        self.rets['data'][t_date] = trade_list
        # self.rets['data'].extend(trade_list)

    def result(self):
        """
        获得收益贡献统计
        :return: df_top_k, df_bottom_k
        """
        self.rets['data_list'] = []
        for _,v in self.rets['data'].values():
            self.rets['data_list'].extend(v)
        df_daily_trade = pd.DataFrame(self.rets['data_list'])
        df_daily_trade = bt_resulst_utils.build_trade_history(df_daily_trade)
        if self.p.contribution_freq not in analysis_util.FREQ_GROUPER_MAP:
            raise ValueError(f"DailyTradeStats - Invalid contribution_freq:{self.p.contribution_freq}")
        df_daily_trade['date'] = pd.to_datetime(df_daily_trade['date'])
        grouper_key = analysis_util.FREQ_GROUPER_MAP[self.p.contribution_freq]
        time_format = analysis_util.FREQ_TIME_FORMAT_REF[self.p.contribution_freq]
        k = int(self.p.k_largest)
        groups = df_daily_trade.groupby(pd.Grouper(key='date', freq=grouper_key))
        top_k_list = []
        bottom_k_list = []
        for date_key, group in groups:
            trade_groups = group.groupby(by=['ref'])
            res_list = []
            for trade_key, trade_group in trade_groups:
                mask = (trade_group['date'] == trade_group['dtclose']) | (trade_group['status'] == 'Open')
                df_t = trade_group[mask]
                if df_t.empty:
                    continue
                pnlcomm_change_0 = df_t['overall_pnlcomm_change'].iloc[0]
                pnl_change_0 = df_t['overall_pnl_change'].iloc[0]
                if pd.isna(pnlcomm_change_0):
                    # 本季度首次建仓
                    pnlcomm_change = df_t['overall_pnlcomm'].iloc[-1]
                    pnl_change = df_t['overall_pnl'].iloc[-1]
                else:
                    pnlcomm_change = df_t['overall_pnlcomm'].iloc[-1] - df_t['overall_pnlcomm'].iloc[
                        0] + pnlcomm_change_0
                    pnl_change = df_t['overall_pnl'].iloc[-1] - df_t['overall_pnl'].iloc[0] + pnl_change_0
                res_list.append({
                    'period_key ': date_key.strftime(time_format),
                    'order_book_id': df_t.iloc[0, 1],
                    'pnl_change': pnl_change,
                    'pnlcomm_change': pnlcomm_change,
                })
            if not res_list:
                logging.info("period_key:" + date_key.strftime(time_format) + "的交易为空")
                continue
            df_period_pnl = pd.DataFrame(res_list).sort_values(by='pnlcomm_change', ascending=False)
            top_k_df = df_period_pnl.head(k).copy()
            bottom_k_df = df_period_pnl.tail(k).copy()
            top_k_df['rank'] = top_k_df['pnlcomm_change'].rank(ascending=False)
            top_k_df['rank_str'] = top_k_df['rank'].apply(lambda x: f"top_{int(x)}")
            bottom_k_df['rank'] = bottom_k_df['pnlcomm_change'].rank(ascending=True)
            bottom_k_df['rank_str'] = bottom_k_df['rank'].apply(lambda x: f"bottom_{int(x)}")
            top_k_list.append(top_k_df)
            bottom_k_list.append(bottom_k_df)
        df_top_k = pd.concat(top_k_list, axis=0)
        df_bottom_k = pd.concat(bottom_k_list, axis=0)
        return {
            'df_top_k': df_top_k,
            'df_bottom_k': df_bottom_k,
            'df_daily_pnl': df_daily_trade,
        }
