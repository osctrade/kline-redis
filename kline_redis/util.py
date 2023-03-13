import logging
import pkgutil

import yaml


def set_logging_config():
    logging_config = pkgutil.get_data(__name__, "logging.yml")
    dict_conf = yaml.safe_load(logging_config)
    logging.config.dictConfig(dict_conf)


def split_list_by_n(list_collection, n):
    """
    将集合均分，每份n个元素
    :param list_collection:
    :param n:
    :return:返回的结果为评分后的每份可迭代对象
    """
    for i in range(0, len(list_collection), n):
        yield list_collection[i: i + n]
