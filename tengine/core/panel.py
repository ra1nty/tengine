from copy import deepcopy
from typing import Optional, Tuple, Any, List, Union
import dataclasses
import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
from tengine.logging import raise_log, raise_if_not, raise_if, get_logger


class Panel(object):
    def __init__(self) -> None:
        pass
    
    @staticmethod
    def from_csv(filename) -> 'Panel':
        pass