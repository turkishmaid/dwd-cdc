#!/usr/bin/env python
# coding: utf-8

"""
Test ftplight module.

Created: 06.09.20
"""

import logging
from ftplib import FTP
from pathlib import Path

import johanna
import ftplight


def cb():
    if not hasattr(cb, "call_cnt"):
        cb.call_cnt = 0
    cb.call_cnt += 1
    if cb.call_cnt < 5:
        logging.info("cb(): raising NotImplementedError =:)")
        raise NotImplementedError(f"cb#{cb.call_cnt}")
    else:
        logging.info("cb(): I'm fine :)")
    pass


def main_repeat():
    ftplight.repeat(cb)
    ftplight.repeat(cb)


# Germany > hourly > Temperaure > hictorical
REMOTE_BASE = "climate_environment/CDC/observations_germany/climate/hourly/air_temperature"
LOCAL = Path("../local")


def get_stationen():
    fnam = "TU_Stundenwerte_Beschreibung_Stationen.txt"
    remote = REMOTE_BASE + "/" + "recent"
    ftp = ftplight.dwd(remote)
    logging.info("-"*80)
    lines = ftplight.ftp_retrlines(ftp, fnam, verbose=True)
    assert isinstance(lines, list), type(lines)
    logging.info(f"got {len(lines)} lines")
    for line in lines[:7]:
        logging.info(line)
    logging.info("...")
    for line in lines[-3:]:
        logging.info(line)
    logging.info("-"*80)
    path = ftplight.ftp_retrlines(ftp, fnam, to_path=LOCAL/fnam, verbose=True)
    assert isinstance(path, Path)
    logging.info(f"path = {path}")
    lines2 = path.read_text().split("\n")
    logging.info(f"{len(lines2)} lines")
    for line in lines2[:7]:
        logging.info(line)
    logging.info("...")
    for line in lines2[-3:]:
        logging.info(line)

    tx = "\n".join(lines[:7])
    tx2 = "\n".join(lines2[:7])
    assert tx == tx2


def get_potsdam():
    remote = REMOTE_BASE + "/" + "historical"
    ftp = ftplight.dwd(remote)
    logging.info("-"*80)
    fnams = ftplight.ftp_nlst(ftp, station=3987)
    fnam = fnams[0]
    target = ftplight.ftp_retrbinary(ftp, fnam, LOCAL/fnam, verbose=True)


if __name__ == "__main__":
    johanna.main(None)
    get_stationen()
    get_potsdam()
