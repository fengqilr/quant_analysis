# -*- coding: utf-8 -*-

import pandas as pd
import sys
sys.path.append("D:\PycharmProjects\quant_analyze\quartz\quantify")
from core import StrategyBase


class factor_strategy(StrategyBase):
    def handle_data(self):
        if len(self.day_hist):
            last_day = self.day_hist[-1]
            today = self.account.current_date
            last_month = int(last_day[4:6])
            this_month = int(today[4: 6])
            if this_month != last_month:
                roe



