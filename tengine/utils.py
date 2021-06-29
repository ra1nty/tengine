from typing import Iterable
import pandas as pd


def find_min_freq(freqs: Iterable) -> str:
    if 'D' in freqs:
        return 'D'
    if 'M' in freqs:
        return 'M'
    if 'Q' in freqs:
        return 'Q'
    if 'Y' in freqs:
        return 'Y'


def df_column_filter(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if 'query' in kwargs:
        return df.query(kwargs['query'])
    else:
        make_filter = lambda k, v: '({}=="{}")'.format(k, v)
        query_str = '&'.join((make_filter(k, v) for k,v in kwargs.items()))
        return df.query(query_str)
