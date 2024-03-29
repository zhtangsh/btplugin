import backtrader as bt
import pandas as pd
from ..utils import analysis_util, bt_resulst_utils


class BktGeneraStatics(bt.Analyzer):
    params = (
        ('timeframe', bt.TimeFrame.Days),
        ('compression', 1),
        ('strategy_freq', 'W'),  # 策略信号频率，用于进行交易结果分析
        ('npv_freq', 'D'),  # 对应日度价格数据
        ('rf', 0.),
        ('future_like', False)
    )

    def __init__(self):
        tr_param = dict(timeframe=self.p.timeframe,
                        compression=self.p.compression)
        self._returns = bt.analyzers.TimeReturn(**tr_param)
        self._positions = bt.analyzers.PositionsValue(headers=True, cash=True)
        self._transactions = bt.analyzers.Transactions(headers=True)

    def stop(self):
        super(BktGeneraStatics, self).stop()
        self.rets['returns'] = self._returns.get_analysis()
        self.rets['positions'] = self._positions.get_analysis()
        self.rets['transactions'] = self._transactions.get_analysis()

    def result(self):
        """
        返回分析器的结果,包括三个Dateframe
        1. 综合分析dataframe
        2. 仓位表
        3. 交易表
        :return:
        """
        # Returns
        cols = ['index', 'return']
        returns = pd.DataFrame.from_records(iter(self.rets['returns'].items()), index=cols[0], columns=cols)
        returns.index = pd.to_datetime(returns.index)
        rets = returns['return']
        # _npv
        _npv = (1 + rets).cumprod()
        # Position value
        p_df = bt_resulst_utils.build_position_value(self.rets['positions'])
        # Transaction value
        t_df = bt_resulst_utils.build_transaction(self.rets['transactions'])
        # Turnover value
        if self.p.future_like:
            turnover = analysis_util.future_average_turnover(t_df)
        else:
            turnover = analysis_util.average_turnover(p_df, t_df, self.p.strategy_freq)
        df_analysis = analysis_util.get_netvalue_analysis(_npv, freq=self.p.npv_freq, rf=self.p.rf)
        df_analysis['年化换手率'] = turnover
        df_npv = pd.DataFrame({
            'npv': _npv,
            'r': rets,
            'maxdrawdowns': analysis_util.get_maxdrawdown(_npv)
        })
        p_df = p_df.reset_index()
        cols = []
        for c in p_df.columns:
            if c == 'date' or c == 'sum':
                continue
            cols.append(c)
        df_p_record = pd.melt(p_df, id_vars=['date'], value_vars=cols, var_name='order_book_id', value_name='position')
        df_yearly_analysis = analysis_util.get_yearly_analysis(_npv, freq=self.p.npv_freq, rf=self.p.rf)
        return {
            'npv': df_npv,
            'analysis': df_analysis,
            'yearly_analysis': df_yearly_analysis,
            'position': df_p_record[df_p_record['position'] > 0].copy().sort_values(by='date'),
            'transaction': t_df
        }
