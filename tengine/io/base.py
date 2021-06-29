from abc import abstractmethod
import configparser
from pathlib import Path
from tengine.core import TimeSeries
from tengine.logging import get_logger, raise_log


logger = get_logger(__name__)


class ReadWriteStore(object):

    def __init__(self, config_file='store.ini') -> None:
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.daemon = {}
        self.__load_plugins()

    def __load_plugins(self) -> None:
        """Load plugins
        """
        pluginstr = self.config['store'].get('enable_plugin')
        if pluginstr is None:
            logger.warning(
                "No plugin loaded, download/update will be disabled", UserWarning)
            return
        plugins = [plugin.strip() for plugin in pluginstr.split(',')]
        from tengine.plugins import load_plugin
        for plugin in plugins:
            self.daemon[plugins] = load_plugin(plugin, **self.config[plugin])

    @abstractmethod
    def __getitem__(self, name) -> TimeSeries:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass
