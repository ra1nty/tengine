import importlib
from abc import ABCMeta, abstractmethod


plugins_mapping = {
    'bloomberg': ('bbg', 'BBGDaemon'),
    'fred': ('fred', 'FredDaemon'),
}


def load_plugin(name: str, **kwargs):
    fname, classname = plugins_mapping[name]
    module = importlib.import_module('tengine.plugins.'+fname)
    klass = getattr(module, classname)
    return klass(**kwargs)


class DataDaemon(metaclass=ABCMeta):
    
    BULK_DOWNLOAD = False

    @abstractmethod
    def download_series(self, ticker: str, start=None, end=None):
        pass

    @abstractmethod
    def download_meta_info(self, ticker: str):
        pass
