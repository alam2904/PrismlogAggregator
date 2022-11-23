from configparser import ConfigParser
from pathlib import Path
# import logging

class ConfigManager:
    def config_file(self):
        config = ConfigParser()
        file = Path('path_config.properties')
        return config.read(file)
#         print(config.sections())
        
# conf = ConfigManager()
# conf.config_file()