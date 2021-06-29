from tengine.logging import get_logger, raise_log


logger = get_logger(__name__)


class Store(object):
    """Factory for Store object

    Args:
        engine ([str]): Engine
    """
    def __init__(self, engine: str='hdf5', config_file: str='store.ini', read_only=False, **kwargs) -> 'Store':
        if engine == 'hdf5':
            from .hdfstore import HDFStore
            return HDFStore(config_file=config_file, **kwargs)
        else:
            raise_log(NotImplementedError('Only support hdf5 engine'))