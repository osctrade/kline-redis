import logging
import pkgutil

import yaml


def set_logging_config():
    logging_config = pkgutil.get_data(__name__, "logging.yml")
    dict_conf = yaml.safe_load(logging_config)
    logging.config.dictConfig(dict_conf)
