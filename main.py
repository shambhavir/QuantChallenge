from QuantConnect.Data.UniverseSelection import *
import math
import numpy as np
import pandas as pd
import scipy as sp
# import statsmodels.api as sm

class FundamentalFactorAlgorithm(QCAlgorithm):

    def Initialize(self):

        self.SetStartDate(2019, 1, 1)  #Set Start Date
        self.SetEndDate(2020, 10, 1)  #Set Start Date       
        self.SetCash(100000)            #Set Strategy Cash
    
        
        self.UniverseSettings.Resolution = Resolution.Daily
        self.AddUniverse(self.CoarseSelectionFunction, self.FineSelectionFunction)
        self.spy = self.AddEquity("SPY", Resolution.Minute).Symbol 
        self.holding_months = 1
        self.num_screener = 100
        self.num_stocks = 30
        self.formation_days = 200
        self.lowmom = False
        self.month_count = self.holding_months
        self.Schedule.On(self.DateRules.MonthStart("SPY"), self.TimeRules.At(0, 0), Action(self.monthly_rebalance))
        self.Schedule.On(self.DateRules.MonthStart("SPY"), self.TimeRules.At(10, 0), Action(self.rebalance))
        # rebalance the universe selection once a month
        self.rebalence_flag = 0
        self.first_month_trade_flag = 1
        self.trade_flag = 0 
        self.symbols = None
 
    def CoarseSelectionFunction(self, coarse):
        if self.rebalence_flag or self.first_month_trade_flag:
            # drop stocks which have no fundamental data or have too low prices
            selected = [x for x in coarse if (x.HasFundamentalData) and (float(x.Price) > 5)]
            # rank the stocks by dollar volume 
            filtered = sorted(selected, key=lambda x: x.DollarVolume, reverse=True) 
    
            return [ x.Symbol for x in filtered[:200]]
        else:
            return self.symbols


    def FineSelectionFunction(self, fine):
        if self.rebalence_flag or self.first_month_trade_flag:
            hist = self.History([i.Symbol for i in fine], 1, Resolution.Daily)
            try:
                filtered_fine = [x for x in fine if (x.ValuationRatios.EVToEBITDA > 0) 
                                                    and (x.EarningReports.BasicAverageShares.ThreeMonths > 0) 
                                                    and float(x.EarningReports.BasicAverageShares.ThreeMonths) * hist.loc[str(x.Symbol)]['close'][0] > 2e9]
            except:
                filtered_fine = [x for x in fine if (x.ValuationRatios.EVToEBITDA > 0) 
                                                and (x.EarningReports.BasicAverageShares.ThreeMonths > 0)] 

            top = sorted(filtered_fine, key = lambda x: x.ValuationRatios.EVToEBITDA, reverse=True)[:self.num_screener]
            self.symbols = [x.Symbol for x in top]
            
            self.rebalence_flag = 0
            self.first_month_trade_flag = 0
            self.trade_flag = 1
            return self.symbols
        else:
            return self.symbols
    
    def OnData(self, data):
        pass
    
    def monthly_rebalance(self):
        self.rebalence_flag = 1

    def rebalance(self):
        spy_hist = self.History([self.spy], 120, Resolution.Daily).loc[str(self.spy)]['close']
        if self.Securities[self.spy].Price < spy_hist.mean():
            for symbol in self.Portfolio.Keys:
                if symbol.Value != "TLT":
                    self.Liquidate(symbol.Value)
            self.AddEquity("TLT")
            self.SetHoldings("TLT", 1)
            return

        if self.symbols is None: return
        chosen_df = self.calc_return(self.symbols)
        chosen_df = chosen_df.iloc[:self.num_stocks]
        
        self.existing_pos = 0
        add_symbols = []
        for symbol in self.Portfolio.Keys:
            if symbol.Value == 'SPY': continue
            if (symbol.Value not in chosen_df.index):  
                self.SetHoldings(symbol, 0)
            elif (symbol.Value in chosen_df.index): 
                self.existing_pos += 1
            
        weight = 0.99/len(chosen_df)
        for symbol in chosen_df.index:
            self.AddEquity(symbol)
            self.SetHoldings(symbol, weight)    
                
    def calc_return(self, stocks):
        hist = self.History(stocks, self.formation_days, Resolution.Daily)
        current = self.History(stocks, 1, Resolution.Minute)
        
        self.price = {}
        ret = {}
     
        for symbol in stocks:
            if str(symbol) in hist.index.levels[0] and str(symbol) in current.index.levels[0]:
                self.price[symbol.Value] = list(hist.loc[str(symbol)]['close'])
                self.price[symbol.Value].append(current.loc[str(symbol)]['close'][0])
        
        for symbol in self.price.keys():
            ret[symbol] = (self.price[symbol][-1] - self.price[symbol][0]) / self.price[symbol][0]
        df_ret = pd.DataFrame.from_dict(ret, orient='index')
        df_ret.columns = ['return']
        sort_return = df_ret.sort_values(by = ['return'], ascending = self.lowmom)
        
        return sort_return
