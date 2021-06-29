import pandas as pd
from fredapi import Fred
from tengine.plugins import DataDaemon
from tengine.core import TimeSeries


class FredDaemon(DataDaemon):
    
    BULK_DOWNLOAD = False

    def __init__(self, api_key):
        if api_key is None:
            raise KeyError('API Key not found for FRED in config file')
        self.api_key = api_key
        self.fred = Fred(api_key=self.api_key)
    
    def download_series(self, ticker, start=None, end=None) -> TimeSeries:
        meta_info = self.download_meta_info(ticker)
        raw_series = self.fred.get_series(ticker, observation_start=start, observation_end=end)
        raw_series = raw_series.to_period(meta_info['frequency_short']).to_timestamp(how='e')
        raw_series.name = ticker
        return TimeSeries(ticker, raw_series.to_frame(), meta_info['frequency_short'], meta_info)
    
    def download_meta_info(self, ticker) -> dict:
        meta_info = self.fred.get_series_info(ticker).to_dict()
        meta_info['source'] = 'fred'
        return meta_info

    @staticmethod
    def reconcile_series(pseries: pd.Series, idx, freq='D') -> pd.Series:
        idx = pseries.index.to_period(freq)
        rseries = pseries.copy()
        rseries.index = idx
        return rseries
