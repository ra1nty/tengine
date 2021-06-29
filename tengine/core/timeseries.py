from copy import deepcopy
from typing import Optional, Tuple, Any, List, Union
import dataclasses
import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
from tengine.logging import raise_log, raise_if_not, raise_if, get_logger


logger = get_logger(__name__)


@dataclasses.dataclass
class TimeSeries(object):
    """Wrapper for all Time Series

    Args:
        name ([str]): Name of the Time Series
        _df ([pd.DataFrame]): Actual data of the time-series
        freq ([str]): A Pandas offset alias
    """
    name: str
    _df: pd.DataFrame = dataclasses.field(repr=False)
    freq: Optional[Union[str, DateOffset]] = None
    meta_info: Optional[dict] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        inferred_freq: str = None
        if isinstance(self._df.index, pd.PeriodIndex):
            inferred_freq = self._df.freq
            self._df.index = self._df.index.to_timestamp(how='e')
        else:
            inferred_freq = pd.infer_freq(self.df.index)
        if inferred_freq is not None:
            if self.freq is not None:
                if self.freq != inferred_freq:
                    if not isinstance(self.freq, DateOffset):
                        logger.warning('The inferred frequency does not match the value of the "freq" argument.\
                                        will resample according to "freq" argument.')
                    self._df = self._df.resample(
                        self.freq).asfreq(fill_value=None)
            else:
                self.freq = inferred_freq
        else:
            if self.freq is not None:
                self._df = self._df.resample(self.freq).asfreq(fill_value=None)
            else:
                raise_log(ValueError('Can not infer frequency, please specify frequency explicitly with "freq" argument.'))
    
    @property
    def df(self):
        return self._df.copy()

    def copy(self) -> 'TimeSeries':
        return TimeSeries(self.name, self._df.copy(), self.freq, deepcopy(self.meta_info))

    def resample(self, freq: Union[str, DateOffset]) -> 'TimeSeries':
        ts = self.copy()
        ts._df = ts._df.resample(freq).asfreq(fill_value=None)
        ts.freq = freq
        return ts

    def __eq__(self, other):
        if isinstance(other, TimeSeries):
            if not self._df.equals(other.df()):
                return False
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self._df)
    
    def __copy__(self):
        return self.copy()
    
    def __deepcopy__(self):
        return self.copy()

    def __getitem__(self, key: Union[pd.DatetimeIndex, List[str], List[int], List[pd.Timestamp], str, int,
                                     pd.Timestamp, Any]) -> 'TimeSeries':
        """Allow indexing on TimeSeries.
        The supported index types are the following base types as a single value, a list or a slice:
        - pd.Timestamp -> return a TimeSeries corresponding to the value(s) at the given timestamp(s).
        - str -> return a TimeSeries including the column(s) specified as str.
        - int -> return a TimeSeries with the value(s) at the given row index.
        `pd.DatetimeIndex` is also supported and will return the corresponding value(s) at the provided time indices.
        .. warning::
            slices use pandas convention of including both ends of the slice.
        """
        def use_iloc(key: Any) -> TimeSeries:
            """return a new TimeSeries from a pd.DataFrame using iloc indexing."""
            return TimeSeries.from_df(self._df.iloc[key], freq=self.freq)

        def use_loc(key: Any, col_indexing: Optional[bool] = False) -> TimeSeries:
            """return a new TimeSeries from a pd.DataFrame using loc indexing."""
            if col_indexing:
                return TimeSeries.from_df(self._df.loc[:, key], freq=self.freq)
            else:
                return TimeSeries.from_df(self._df.loc[key], freq=self.freq)

        if isinstance(key, pd.DatetimeIndex):
            check = np.array([elem in self._df.index() for elem in key])
            if not np.all(check):
                raise_log(IndexError(
                    "None of {} in the index".format(key[~check])), logger)
            return use_loc(key)
        elif isinstance(key, slice):
            if isinstance(key.start, str) or isinstance(key.stop, str):
                return use_loc(key, col_indexing=True)
            elif isinstance(key.start, int) or isinstance(key.stop, int):
                return use_iloc(key)
            elif isinstance(key.start, pd.Timestamp) or isinstance(key.stop, pd.Timestamp):
                return use_loc(key)
        elif isinstance(key, list):
            if all(isinstance(s, str) for s in key):
                return use_loc(key, col_indexing=True)
            elif all(isinstance(i, int) for i in key):
                return use_iloc(key)
            elif all(isinstance(t, pd.Timestamp) for t in key):
                return use_loc(key)
        else:
            if isinstance(key, str):
                return use_loc([key], col_indexing=True)
            elif isinstance(key, int):
                return use_iloc([key])
            elif isinstance(key, pd.Timestamp):
                return use_loc([key])

        raise_log(IndexError("The type of your index was not matched."), logger)
