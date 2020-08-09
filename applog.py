#!/usr/bin/env python
# coding: utf-8

"""
Application level logging.
"""
# Created: 12.06.20

import sys
import logging
from logging.handlers import RotatingFileHandler

import dotfolder, mailgun

_LOGGING_FMT = "%(asctime)s [%(levelname)s] %(message)s"

# simple container for singleton data
# cf. https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
_ROTATING_FILE_PATH = None
_ROTATING_FILE_HANDLER = None
_STDOUT_HANDLER = None
_FILE_PATH = None
_FILE_HANDLER = None


def init_logging(collective: bool = False, console: bool = False, process: bool = False) -> None:
    """
    Logs shall be initialized as one of the first steps in bootstrapping.
    While all options default to False, at least one must be true. If none is supplied,
    console log will be enabled.

    :param collective: write to a set of rotating logfiles in ~/.luechenbresse
    :param console: write to console (stdout)
    :param process: write to a logfile unique to this process
    :return: nothing. However, a first log message is emitted.
    """
    # TODO run this on import?
    # TODO use https://pypi.org/project/concurrent-log-handler/ instead of RotatingFileHandler
    # DONE on/off for log handlers via parameter to support e.g. notebooks that do not do runwise logging

    global _ROTATING_FILE_PATH, _ROTATING_FILE_HANDLER, _STDOUT_HANDLER, _FILE_PATH, _FILE_HANDLER

    # avoid blunt abuse
    if _ROTATING_FILE_PATH or _STDOUT_HANDLER or _FILE_HANDLER:
        return

    # we need some device at least
    if not collective and not process:
        console = True

    handlers = []
    remark = []

    if collective:
        # default.log with 10 rotating segments of 100k each -> 1 MB (reicht viele Tage)
        _ROTATING_FILE_PATH = dotfolder.dotfolder() / "default.log"
        _ROTATING_FILE_HANDLER = RotatingFileHandler(_ROTATING_FILE_PATH, maxBytes=100_000, backupCount=10)
        _ROTATING_FILE_HANDLER.setLevel(logging.INFO)
        handlers.append(_ROTATING_FILE_HANDLER)
        remark.append("collective")

    if console:
        # console output TODO how can we switch off via ini-file?
        _STDOUT_HANDLER = logging.StreamHandler(sys.stdout)
        _STDOUT_HANDLER.setLevel(logging.DEBUG)
        handlers.append(_STDOUT_HANDLER)
        remark.append("console")

    if process:
        # file for output of the current run, will be sent via mail
        _FILE_PATH = dotfolder.dotfolder() / "current.log"
        _FILE_HANDLER = logging.FileHandler(_FILE_PATH, mode="w")
        _FILE_HANDLER.setLevel(logging.DEBUG)
        handlers.append(_FILE_HANDLER)
        remark.append("process")

    # noinspection PyArgumentList
    logging.basicConfig(level=logging.INFO, handlers=handlers, format=_LOGGING_FMT)
    logging.info(f"LogManager lebt. ({','.join(remark)})")


def shoot_mail(subject="von luechenbresse with love"):
    global _FILE_HANDLER

    # close current.log
    # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
    if not _FILE_HANDLER:
        raise Exception("Cannot send mail without content in process logger.")

    logger = logging.getLogger()
    logger.removeHandler(_FILE_HANDLER)
    _FILE_HANDLER = None
    logging.info(f'closed {_FILE_PATH}')

    # send file contents via email
    # https://realpython.com/python-pathlib/#reading-and-writing-files
    body = _FILE_PATH.read_text()
    # logger.addHandler(_FILE_HANDLER) TODO not throw away but append after reading the contents

    mailgun.shoot_mail(subject, body)

