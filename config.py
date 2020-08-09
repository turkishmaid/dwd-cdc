#!/usr/bin/env python
# coding: utf-8

"""
Convenience API for the ini file

Created: 09.08.20
"""

import configparser
from pathlib import Path
import logging

import dotfolder

_CONFIG = None


def config_file():
    return dotfolder.dotfolder() / dotfolder.CONFIGNAME # TODO clean code!


def read():
    global _CONFIG
    if not _CONFIG:
        ini_file = config_file()
        _CONFIG = configparser.ConfigParser()
        _CONFIG.read(ini_file)


def get(section, key, default=None):
    read()
    if section in _CONFIG:
        if key in _CONFIG[section]:
            return _CONFIG[section][key]
    return default


def create_ini_file(db_folder: str = None) -> None:
    """
    Create ~/.luechenbresse/luechenbresse.ini configuration file.
    Create missing databases.
    Databases will be placed beside, when no other directory is specified.
    Moving the databases later can be done by adapting [databases]->folder in the ini-file
    and moving the database files manually.

    :param db_folder: folder where databases are located, default: ~/.luechenbresse
    :return:
    """
    pass

if __name__ == "__main__":
    pass