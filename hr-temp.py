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
# Created: 09.08.20

from time import perf_counter, process_time
import json
import tracemalloc
import logging
from ftplib import FTP
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
import os

from docopt import docopt, DocoptExit, DocoptLanguageError

import dotfolder
import applog
import config


def ls(path: Path) -> None:
    """
    Listet das angegebene Verzeichnis in zeitlicher Sortierung ins log.
    Cortesy https://linuxhandbook.com/execute-shell-command-python/

    :param path: das zu listende Verzeichnis
    """
    console = os.popen(f'cd {path}; ls -latr').read()
    logging.info(f"ls -la {path}:\n" + console)


class Connection:
    # manages SQLite connection and cursor to avoid caring for the name in many routines

    def __init__(self):
        self._db = Path(config.get("databases", "folder")) / "hr-temp.sqlite"

    def __enter__(self):
        """
        cur und con sind public für den Verwender
        """
        self.conn = sqlite3.connect(self._db)
        self.cur = self.conn.cursor()
        return self

    def commit(self):
        self.conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.cur = None
        self.conn = None


class ProcessStationen:
    # ein File, alle Stationen (Stammdaten), kapselt den Ursprungsort der Liste

    def __init__(self):
        self._download()
        self._upsert()

    # TODO absichern mit try und sleep/retry, kein Programmabbruch bei Fehler
    def _download(self) -> None:
        with applog.Timer() as t:
            ftp = FTP("opendata.dwd.de")
            ftp.login()  # anonymous
            ftp.cwd("climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent")
            logging.info(f"zum DWD konnektiert {t.read()}")
            self.rows = list()
            self.cnt = 0
            rt = ftp.retrlines("RETR TU_Stundenwerte_Beschreibung_Stationen.txt", self._collect)
            logging.info(rt)  # like "226 Directory send OK."
            logging.info(f"{self.cnt} Stationen gelesen und geparst {t.read()}")
            ftp.quit()
        logging.info(f"Verbindung zum DWD geschlossen {t.read()}")

    def _collect(self, line: str) -> None:
        """
        Callback für FTP.retrlines
        :param line: Zeile der Stationsliste = Station (oder Überschrift)
        """
        def iso_date(s: str) -> str:
            """
            Formatiert ds CDC Datum als ISO-Datum. Bsp.: 19690523 -> 1969-05-23
            :param s: Datum in Schreibweise YYYYMMDD
            :return: Datum in Schreibweise YYYY-MM-DD
            """
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
                iso_date(parts[1]),    # yymmdd_von text,
                iso_date(parts[2]),    # yymmdd_bis text,
                int(parts[3]),          # hoehe integer,
                float(parts[4]),        # breite real,
                float(parts[5]),        # laenge real,
                " ".join(parts[6:-1]),  # name text,
                parts[-1]               # (bundes)land text
            )
            self.rows.append(tup)
            self.cnt += 1

    def _upsert(self):
        with applog.Timer() as t:
            with Connection() as c:
                # https://database.guide/how-on-conflict-works-in-sqlite/
                c.cur.executemany("""
                    INSERT OR REPLACE INTO stationen
                    VALUES (?,?,?,?,?,?,?,?)
                """, self.rows)
                c.commit()
        logging.info(f"{self.cnt} Stationen in die Datenbank geschrieben {t.read()}")


class FtpFileList:
    # bestimmt alle .zip-Dateien in einem FTP-Verzeichnis

    def __init__(self, ftp: FTP):
        """
        :param ftp: geöffnete FTP Verbindung mit dem richtigen Arbeitsverzeichnis
        """
        self._download(ftp)

    def _download(self, ftp: FTP) -> None:
        """
        :param ftp: geöffnete FTP Verbindung mit dem richtigen Arbeitsverzeichnis
        """
        # retrieve list of .zip files to download
        self._zips = list()
        self._cnt = 0
        with applog.Timer() as t:
            rt = ftp.retrlines("NLST *.zip", callback=self._collect)
        logging.info(rt)      # like "226 Directory send OK."
        logging.info(f"{self._cnt} Filenamen gelesen {t.read()}")

    def _collect(self, fnam: str) -> None:
        """
        Callback für FTP.retrlines.
        :param fnam: Zeile der Fileliste = Filename
        """
        self._zips.append(fnam)
        self._cnt += 1

    def get(self) -> list:
        """
        Gibt das Ergebnis des Downloads zurück.
        :return: Liste von Zip-Filenamen
        """
        return self._zips


class ProcessDataFile:
    # Downloads, parses and saves a CDC data file from an open FTP connection

    def __init__(self, ftp: FTP, fnam: str, verbose: bool = False):
        """
        :param ftp: geöffnete FTP Verbindung mit dem richtigen Arbeitsverzeichnis
        :param fnam: Name des herunterzuladenden Files
        :param verbose: Konsolenausgabe als Fortschrittinfo -- DO NOT USE IN PRODUCTION
        """
        self._verbose = verbose
        logging.info(f'DataFile(_,"{fnam}")')
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            logging.info(f"Temporäres Verzeichnis: {temp_dir}")
            zipfile = temp_dir / fnam
            self._download(ftp, fnam, zipfile)
            ls(temp_dir)

        if temp_dir.exists():
            applog.ERROR = True
            logging.error(f"Temporäres Verzeichnis {temp_dir} wurde NICHT entfernt")

    def _download(self, ftp: FTP, from_fnam: str, to_path: Path) -> None:
        """
        :param ftp: geöffnete FTP Verbindung mit dem richtigen Arbeitsverzeichnis
        :param from_fnam: Name des herunterzuladenden Files
        :param to_path: Speicherort lokal
        """
        self._cnt = 0
        self._volume = 0
        # TODO absichern mit try und sleep/retry, kein Programmabbruch bei Fehler
        with applog.Timer() as t:
            with open(to_path, 'wb') as self.open_zip_file:
                ftp.retrbinary("RETR " + from_fnam, self._collect)
            if self._verbose:
                print()  # awkward
        logging.info(f"Zipfile heruntergeladen: {self._volume:,} Bytes in {self._cnt} Blöcken {t.read()}")
        assert to_path.exists()

    def _collect(self, b: bytes) -> None:
        """
        Callback für FTP.retrbinary
        :param b:
        """
        self.open_zip_file.write(b)
        self._cnt += 1
        self._volume += len(b)
        tick = (self._cnt % 100 == 0)
        if self._verbose and tick:
            print(".", end="", flush=True)


# Germany > hourly > Temperaure > hictorical
SERVER = "opendata.dwd.de"
REMOTE_BASE = "climate_environment/CDC/observations_germany/climate/hourly/air_temperature"

def process_dataset(kind: str) -> None:

    remote = REMOTE_BASE + "/" + kind
    with applog.Timer() as t:
        ftp = FTP(SERVER)
        ftp.login()  # anonymous
        ftp.cwd(remote)
    logging.info(f"Connectiert an {SERVER} pwd={remote} {t.read()}")

    file_list = FtpFileList(ftp).get()
    for fnam in file_list[:1]:
        ProcessDataFile(ftp, fnam, verbose=True)
    hurz = 17  # für Brechpunkt


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
                ProcessStationen()

            if args["--historical"]:
                process_dataset("historical")
                # raise RuntimeError("download of historical data is not yet implemented")

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
