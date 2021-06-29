import datetime
import pandas as pd
from xbbg import blp
from tengine.plugins import DataDaemon
from tengine.core import TimeSeries

class BBGDaemon(DataDaemon):
    BULK_DOWNLOAD = False
    BBG_FREQ_MAP = {
        'Intraday' : 'D', # Intraday not supported, will truncated to Daily
        'Daily' : 'D',
        'Weekly' : 'W',
        'Monthly' : 'M',
        'Quarterly': 'Q',
        'Yearly' : 'Y',
    }

    def __init__(self, freq_limit=None, freq_defualt='D', field=None, start_default=None):
        self.blp = blp
        self.freq_limit = freq_limit
        self.field = field if field is not None else 'px_last'
        self.freq_default = freq_defualt
        self.start_default = start_default
    
    def download_series(self, ticker, start=None, end=None) -> TimeSeries:
        freq = self.freq_default
        kwargs_blp = {}
        meta_df = pd.DataFrame()
        if ticker.split(' ')[-1] == 'Index':
            meta_df = self.blp.bdp(tickers=ticker, flds=['indx_freq', 'history_start_dt'])
            if 'indx_freq' in meta_df:
                freq = self.BBG_FREQ_MAP[meta_df['indx_freq'][ticker]]
            if 'history_start_dt' in meta_df and start is None:
                start = meta_df['history_start_dt'][ticker] # Override BDH default length
        kwargs_blp['Per'] = freq
        if start is None:
            start = self.start_default
        if self.freq_limit:
            kwargs_blp['Per'] = self.freq_limit
        if start is not None:
            kwargs_blp['start_date'] = start
        if end is not None:
            kwargs_blp['end_date'] = end
        raw_series = self.blp.bdh(tickers=ticker, flds=[
                                  self.field], **kwargs_blp).xs(self.field, axis=1, level=1)[ticker]
        raw_series = raw_series.to_period(freq).to_timestamp(how='e')
        meta_info = meta_df.to_dict()
        meta_info['source'] = 'bbg'
        return TimeSeries(ticker, raw_series.to_frame(), kwargs_blp['Per'], meta_info)

    def download_meta_info(self, ticker: str):
        return super().download_meta_info(ticker)
