import backtrader as bt


class BktGeneraStatics(bt.Analyzer):
    params = (
        ('timeframe', bt.TimeFrame.Days),
        ('compression', 1)
    )

    def start(self):
        self._returns = TimeReturn(**dtfcomp)

    def stop(self):
        super(BktGeneraStatics, self).stop()
        self.rets['returns'] = self._returns.get_analysis()

    def result(self):
        # Returns
        cols = ['index', 'return']
        returns = DF.from_records(iteritems(self.rets['returns']),
                                  index=cols[0], columns=cols)
        returns.index = pandas.to_datetime(returns.index)
        rets = returns['return']
        # _npv
        _npv = (1 + _returns).cumprod()
        df_analysis = self.get_netvalue_analysis(_npv, freq='d', rf=0.)
        return df_analysis

    def get_netvalue_analysis(self, netvalue, freq, rf):
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
        if freq == 'D' or freq == '1D':
            oneyear = 252
        elif freq == 'W' or freq == '1W':
            oneyear = 50
        elif freq == 'M':
            oneyear = 12
        elif freq == 'HD':
            oneyear = 2
        elif freq == 'Y':
            oneyear = 1
        else:
            raise ValueError('get_netvalue_analysis -- Not Right freq : ', freq)
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
            drawdowns = RetAnalysis.get_maxdrawdown(netvalue)
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
