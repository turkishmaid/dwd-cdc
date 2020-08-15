#!/usr/bin/env python
# coding: utf-8

"""
Download DWD Stundenwerte Lufttemperatur 2m.

Usage:
  hr-temp.py ( --recent | --historical | --stations )

Options:
  -h --help         Zeige die Bedeutung der Parameter
  --recent          Download der letzten ca. 500 Tage
  --historical      Download des historischen Datenbestandes (das setzt eine
                    leere Datenbank voraus, prüft es aber nicht)
  --stations        Download der Stationsliste
  --limit=LIMIT     Maximal so viele Stationen herunterladen (Testhilfe) [default: -1]
  --skipdl          Dateien nicht herunterladen, wenn sie schon da sind (Testhilfe)
  --skiprm          Dateien nach dem Einarbeiten nicht löschen (Testhilfe)
"""

"""
Hourly Air Temperature 2m

Created: 09.08.20
"""

import sys
import os
from time import time, perf_counter, process_time
import json
import tracemalloc
import logging
from ftplib import FTP
import sqlite3
from pathlib import Path

from docopt import docopt, DocoptExit, DocoptLanguageError

import dotfolder
import applog
import config


class Connection:
    # manages SQLite connection and cursor to avoid caring for the name in many routines

    def __init__(self):
        self.db = Path(config.get("databases", "folder")) / "hr-temp.sqlite"

    def __enter__(self):
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        return self

    def commit(self):
        self.conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.cur = None
        self.conn = None


class Stationen:

    def __init__(self):
        self.cnt = 0
        self.rows = list()

    # callback for ftp.retrlines()
    def collect(self, line):
        def good_date(s):
            # 19690523 -> 1969-05-23
            return "%s-%s-%s" % (s[0:4], s[4:6], s[6:])

        if line.startswith("Stations_id") or line.startswith("-----------"):
            pass
        else:
            """
            Format:
                     1         2         3         4         5         6         7         8         9        10
            ....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|....,....|
            04692 20080301 20181130            229     50.8534    7.9966 Siegen (Kläranlage)                      Nordrhein-Westfalen
            """
            parts = line.split()
            tup = (
                # Tabelle stationen
                int(parts[0]),          # station integer,
                good_date(parts[1]),    # yymmdd_von text,
                good_date(parts[2]),    # yymmdd_bis text,
                int(parts[3]),          # hoehe integer,
                float(parts[4]),        # breite real,
                float(parts[5]),        # laenge real,
                " ".join(parts[6:-1]),  # name text,
                parts[-1]               # (bundes)land text
            )
            self.rows.append(tup)
            self.cnt += 1

    def load(self):
        with applog.Timer() as t:

            ftp = FTP("opendata.dwd.de")
            ftp.login()  # anonymous
            ftp.cwd("climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent")
            logging.info(f"zum DWD konnektiert {t.read()}")
            self.rows = list()
            rt = ftp.retrlines("RETR TU_Stundenwerte_Beschreibung_Stationen.txt", self.collect)
            logging.info(rt)  # like "226 Directory send OK."
            logging.info(f"{self.cnt} Stationen gelesen und geparst {t.read()}")
            ftp.quit()
            logging.info(f"Verbindung zum DWD geschlossen {t.read()}")

            t.reset()
            with Connection() as c:
                # https://database.guide/how-on-conflict-works-in-sqlite/
                c.cur.executemany("""
                    INSERT OR REPLACE INTO stationen
                    VALUES (?,?,?,?,?,?,?,?)
                """, self.rows)
                c.commit()
            logging.info(f"{self.cnt} Stationen in die Datenbak geschrieben {t.read()}")


OPCODE = None     # enable one for interactive debugging in IDE w/o using run configurations
# OPCODE = "recent"
# OPCODE = "historical"
# OPCODE = "stations"

if __name__ == "__main__":
    tracemalloc.start()
    pc0 = perf_counter()
    pt0 = process_time()
    dotfolder.ensure_dotfolder()
    applog.init_logging(collective=False, console=True, process=True)
    dotfolder.init_dotfolder()
    logging.info("Willkommen beim DWD.")
    try:
        try:
            # support interactive debugging
            if OPCODE:
                args = {
                    "--recent": OPCODE == "recent",
                    "--historical": OPCODE == "historical",
                    "--stations": OPCODE == "stations"
                }
                logging.info(f"interactive debugging, OPCODE={OPCODE}")
            else:
                args = docopt(__doc__, version='Download DWD Stundenwerte Lufttemperatur 2m – v0.1')
                logging.info(json.dumps(args, indent=4))

            if args["--stations"]:
                s = Stationen()
                s.load()

            if args["--historical"]:
                raise RuntimeError("download of historical data is not yet implemented")

            if args["--recent"]:
                raise RuntimeError("download of recent data is not yet implemented")

        except DocoptExit as ex:
            applog.ERROR = True
            logging.exception("DocoptExit")
        except DocoptLanguageError as ex:
            applog.ERROR = True
            logging.exception("DocoptLanguageError")
        logging.info("Time total: %0.1fs (%0.1fs process)" % (perf_counter() - pc0, process_time() - pt0))
        current, peak = tracemalloc.get_traced_memory()
        logging.info("Memory: current = %0.1f MB, peak = %0.1f MB" % (current / 1024.0 / 1024, peak / 1024.0 / 1024))
    except KeyboardInterrupt:
        applog.ERROR = True
        logging.warning("caught KeyboardInterrupt")
    except Exception as ex:
        applog.ERROR = True
        logging.exception("Sorry.")
    applog.shoot_mail("Stundenwerte Lufttemperatur 2m - DWD-CDC Download")
    logging.info("Ciao.")
    print()
    print()