#!/usr/bin/env python
# coding: utf-8

"""
Collect code that works with the files in teh .dwd-cdc folder directly.

Created: 09.08.20
"""

import sys
import os
from pathlib import Path
import sqlite3
import logging
import logging.handlers
import configparser

DOTFOLDER = ".dwd-cdc"
CONFIGNAME = "dwd-cdc.ini"


def dotfolder():
    return Path(os.environ["HOME"]) / DOTFOLDER


def ensure_dotfolder():
    # ohne logging
    config_folder = dotfolder()
    print(f"Using '{config_folder}'.")
    if not config_folder.exists():
        print("Created it.")
        config_folder.mkdir()
    return config_folder

# def ensure_dotfolder():
#     folder = dotfolder()
#     print(f"Using '{folder}'.")
#     if not folder.exists():
#         folder.mkdir()
#         return False
#     return True


def _create_db(db_folder, db_name, schema):
    # TODO support more than one schema per db
    if isinstance(schema, list):
        schema = schema[0]
    sql_path = Path(".") / "schema" / (schema + ".sql")
    sql = sql_path.read_text()
    db_path = Path(db_folder) / (db_name + ".sqlite")
    # TODO create table nur wenn keine da ist -> nachdenken!
    logging.info(f"creating DB artifacts from {sql_path}")
    con = sqlite3.connect(db_path.resolve())
    cur = con.cursor()
    cur.executescript(sql)
    con.close()


def init_dotfolder():
    config_folder = ensure_dotfolder()

    # create init-file in dotfolder
    ini_file = config_folder / CONFIGNAME
    config = configparser.ConfigParser()
    if ini_file.exists():
        logging.info(f"Configuration file: {ini_file} OK")
    else:
        logging.info(f"Configuration file: {ini_file} will be created")
        ini_file.touch()
    config.read(ini_file)

    # create database folder.
    # You can
    #   1. create another
    #   2. move DB files to there and
    #   3. adapt [databases]folder
    # at any point in time (between scheduled runs) if you like.
    #
    if not config.has_section("databases"):
        config.add_section("databases")
    if "folder" in config["databases"]:
        db_folder = Path(config["databases"]["folder"])
    else:
        logging.info(f"Defaulting database folder to {config_folder}")
        db_folder = config_folder
        config["databases"]["folder"] = str(config_folder)
        with open(ini_file, "w") as fp:
            config.write(fp)
    logging.info(f"Databases go to {db_folder}")
    if db_folder.exists():
        logging.info("Using existing folder")
    else:
        logging.info(f"Creating {db_folder}...")
        db_folder.mkdir()

    # TODO how to deal with structural updates
    _create_db(db_folder, "hr-temp", "hr-temp-00")


if __name__ == "__main__":
    pass

