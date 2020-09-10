#!/usr/bin/env python
# coding: utf-8

"""
Simple FTP interface, way tailored towards the DWD CDC FTP server.

Why not https://ftputil.sschwarzer.net/trac/wiki/WikiStart
for the various FTP accesses and https://github.com/jd/tenacity
for repeat()? Well, why not keep it simple.

Created: 06.09.20
"""

from ftplib import FTP
import logging
from time import sleep
from typing import Union, Tuple, Any, List
from pathlib import Path

import johanna


def get_station_match(station: int = None) -> str:
    return f"*_{station:05d}_*.zip" if station else "*.zip"


def dwd(folder):
    # TODO make this a Context Handler
    SERVER = "opendata.dwd.de"
    with johanna.Timer() as t:
        ftp = FTP(SERVER, timeout=15)
        ftp.login()  # anonymous
        ftp.cwd(folder)
    logging.info(f"Connected to ftp://{SERVER}/{folder} {t.read()}")
    return ftp


def repeat(callback, do_times: int = 3, throttle_sec: float = 3.0) -> Union[Tuple[bool, None], Tuple[bool, Any]]:
    """
    Repeat callback n times, with throttling to tame external resource access.
    Utilizes https://stackoverflow.com/questions/2083987/how-to-retry-after-exception/7663441#7663441
    :param callback: Function w/o parameters, may raise Exceptions or TimeoutError.
    :param do_times: Try at most that often to execute callback().
    :param throttle_sec: Wait a short time before executing callback().
    :return: (True, result of callback) if operation was successful, else (False, None)
    """
    assert isinstance(do_times, int)
    assert 0 < do_times <= 10
    assert isinstance(throttle_sec, float)
    assert 0.0 <= throttle_sec <= 10.0

    logging.info(f"Retrying max. {do_times} times ...")
    for attempt in range(do_times):
        try:
            result = callback()
        # The exceptions are tuned for callbacks using FTP
        except TimeoutError as ex:  # will hopefully catch socket timeout
            logging.exception("Timeout!")
        except Exception as ex:  # will not catch KeyboardInterrupt :)
            logging.exception("Exception!")
        else:  # executed when the execution falls thru the try
            break
        logging.info(f"Retrying after {throttle_sec:0.1f} sec ...")
        sleep(throttle_sec)
    else:
        johanna.flag_as_error()
        logging.error(f"Finally failed.")
        return False, None
    return True, result


def ftp_nlst(ftp: FTP, station: int = None) -> list:
    """
    Retrieve list of matching file names.
    :param ftp: Mandatory open FTP connection in proper subdirectory.
    :param station: Optional numeric station number. Return all ststion files
        if not specified.
    :return: List of file names (maybe empty) or None in case of issues.
    """

    def collect(fnam: str) -> None:  # Callback for FTP.retrlines
        collect.zips.append(fnam)
        johanna.collect_stat("ftp_bytes", len(fnam))

    def download() -> list:
        collect.zips = list()
        with johanna.Timer() as t:
            rt = ftp.retrlines(f"NLST {station_match}", callback=collect)
        logging.info(rt)  # like "226 Directory send OK."
        logging.info(f"Retrieved {len(collect.zips)} filenames {t.read()}")
        johanna.collect_stat("ftp_sec", t.read(raw=True))
        johanna.collect_stat("ftp_file_cnt", 1)
        return collect.zips

    station_match = get_station_match(station)
    logging.info(f"FTP: trying ot NLST {station_match}")
    success, fnams = repeat(download, do_times=3, throttle_sec=3.0)
    if not success:
        logging.info(f"Cannot retrieve file list for {station_match}.")
        # None will be returned, not empty list
    return fnams


def ftp_retrbinary(ftp: FTP, from_fnam: str, to_path: Path, verbose: bool = False) -> Path:
    """
    Download file to local.
    :param ftp: Mandatory open FTP connection in proper subdirectory.
    :param from_fnam: Mandatory file name for download.
    :param to_path: Mandatory local Path to download file to.
    :param verbose: Print tick per 100 packages.
    :return: Path of downloaded file or None on failure.
    """

    def collect(b: bytes) -> None:  # Callback für FTP.retrbinary
        collect.open_file.write(b)
        collect.cnt += 1
        collect.volume += len(b)
        tick = (collect.cnt % 100 == 0)
        if verbose and tick:
            print(".", end="", flush=True)

    def download() -> Path:
        collect.cnt = 0
        collect.volume = 0
        with johanna.Timer() as t:
            with open(to_path, 'wb') as collect.open_file:
                rt = ftp.retrbinary("RETR " + from_fnam, collect)
            if verbose:
                print()  # awkward
        logging.info(rt)
        logging.info(f"Downloaded {collect.volume:,} bytes in {collect.cnt} blocks {t.read()}")
        johanna.collect_stat("download_bytes", collect.volume)
        johanna.collect_stat("download_time", t.read(raw=True))
        johanna.collect_stat("files_cnt", 1)
        return to_path

    logging.info(f"FTP: trying to RETR {from_fnam} in BINARY mode ...")
    success, path = repeat(download, do_times=3, throttle_sec=3.0)
    if not success:
        logging.info(f"Cannot retrieve file {from_fnam}.")
        # None will be returned, not target path
    return path


def ftp_retrlines(ftp: FTP, from_fnam: str, to_path: Path = None, verbose: bool = False) -> Union[Path, List[str]]:
    """
    Download file to local.
    :param ftp: Mandatory open FTP connection in proper subdirectory.
    :param from_fnam: Mandatory file name for download.
    :param to_path: Path to download file to. If None a list(str) will be returned.
    :param verbose: Print tick per 100 lines.
    :return: Path of downloaded file or list(str) – or None on failure.
    """

    def collect(s: str) -> None:  # Callback für FTP.retrlines
        if to_path:
            collect.open_file.write(s + "\n")
            collect.cnt += 1
            collect.volume += len(s) + 1
        else:
            collect.lines.append(s)
            collect.cnt += 1
            collect.volume += len(s) + 1
        tick = (collect.cnt % 100 == 0)
        if verbose and tick:
            print(".", end="", flush=True)

    def download() -> Path:
        collect.cnt = 0
        collect.volume = 0
        with johanna.Timer() as t:
            if to_path:
                with open(to_path, 'w') as collect.open_file:
                    rt = ftp.retrlines("RETR " + from_fnam, collect)
            else:
                collect.lines = []
                rt = ftp.retrlines("RETR " + from_fnam, collect)
            if verbose:
                print()  # awkward
        logging.info(rt)
        logging.info(f"Downloaded {collect.volume:,} bytes in {collect.cnt} lines {t.read()}")
        johanna.collect_stat("download_bytes", collect.volume)
        johanna.collect_stat("download_time", t.read(raw=True))
        johanna.collect_stat("files_cnt", 1)
        return to_path if to_path else collect.lines

    logging.info(f"FTP: trying to RETR {from_fnam} in TEXT mode ...")
    success, path = repeat(download, do_times=3, throttle_sec=3.0)
    if not success:
        logging.info(f"Cannot retrieve file {from_fnam}.")
        # None will be returned, not target path or file content
    return path


if __name__ == "__main__":
    pass
