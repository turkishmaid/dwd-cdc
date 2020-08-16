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
from zipfile import ZipFile
import csv

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
    """
    Manages SQLite connection and cursor to avoid caring for the name in many routines.
    Nur als Context Handler verwendbar!
    """

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


NULLDATUM = "1700010100"      # Früher als alles. 1970 geht ja beim DWD nicht :)

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
        with applog.Timer() as t:
            with TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                logging.info(f"Temporäres Verzeichnis: {temp_dir}")
                zipfile_path = temp_dir / fnam
                self._download(ftp, fnam, zipfile_path)
                produkt_path = self._extract(zipfile_path, temp_dir)
                with Connection() as c:
                    station, readings = self._parse(produkt_path, c)
                    if readings:
                        self._insert_readings(readings, c)
                        last_date = self._update_recent(readings, c)
                        c.commit()  # gemeinsamer commit ist sinnvoll
                # ls(temp_dir)
                # hurz = 17
        logging.info(f"Werte für Station {station} verarbeitet {t.read()}")

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

    def _extract(self, zipfile_path: Path, target_path: Path) -> Path:
        """
        Die zip-Files enthalten jeweils eine Reihe von html und txt Dateien,
        die die Station und ihre Messgeräte beschreiben. Für den Zweck dieser
        Auswertungen hier sind das urban legends, die getrost ignoriert werden
        können. Die Nutzdaten befinden sich in einem CSV File produkt_*.txt.
        Dieses wird ins tmp/-Verzeichnis extrahiert.

        :param zipfile_path: Path der zu entpackenden zip-Files
        :param target_path: Path des Zielverzeichnisses zum entpacken
        :return: Path des Datenfiles
        """
        zipfile = ZipFile(zipfile_path)
        for zi in zipfile.infolist():
            if zi.filename.startswith("produkt_"):
                produkt = zipfile.extract(zi, path=target_path)
                logging.info(f"Daten in {produkt}")
                return Path(produkt)
        raise ValueError(f"Kein produkt_-File in {zipfile_path}")

    def _parse(self, produkt_path: Path, c: Connection) -> list:
        """
        Parsen des Datenfiles.
        :param produkt_path: Pfad des Datenfiles
        :param c: Connection zur Datenbank
        :return: Stationsnummer und eine Liste von Tupeln, die in die Tabelle readings eingefügt werden können
        """

        def ymdh(yymmddhh: str) -> tuple:
            """
            Aufbrechen der DWD Zeitangabe in numerische Zeiteinheiten.
            :param yymmddhh: Stunde in DWD-Format
            :return: Tuple mit den numerischen Teilkomponenten
            """
            Y = int(yymmddhh[:4])
            M = int(yymmddhh[4:6])
            D = int(yymmddhh[6:8])
            H = int(yymmddhh[-2:])
            return Y, M, D, H

        stem = produkt_path.stem  # like "produkt_tu_stunde_19500401_20110331_00003"
        _, _, _, von, bis, station = stem.split("_")
        station = int(station)
        # Bestimme letzen vorhandenen Wert
        with applog.Timer() as t:
            c.cur.execute("SELECT yyyymmddhh FROM recent where station = ?", (station,))
            surpress_up_to = NULLDATUM
            for row in c.cur:
                if row[0]:
                    surpress_up_to = row[0]
        logging.info(f"Station {station} (Daten bis {surpress_up_to} bereits vorhanden) {t.read()}")
        with applog.Timer() as t:
            readings = list()
            with open(produkt_path, newline='') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=';')
                cnt = 0
                shown = 0
                skipped = -1
                for row in spamreader:
                    cnt += 1
                    if cnt == 1:                    # skip header line
                        header = row
                        continue
                    Y, M, D, H = ymdh(row[1])
                    tup = (
                        int(row[0]),  # station
                        Y, M, D, H,  # row[1],
                        int(row[2]),  # q
                        None if row[3].strip() == "-999" else float(row[3]),  # temp
                        None if row[4].strip() == "-999" else float(row[4])  # humid
                    )
                    # surpress data that might be in DB already
                    if row[1] <= surpress_up_to:
                        continue
                    elif skipped == -1:  # now uncond.
                        skipped = cnt - 2  # current and first excluded
                        logging.info(f"{skipped} Messwerte vor dem {surpress_up_to} wurden übersprungen")
                    if shown <= 1: # show first 2 rows taken
                        shown += 1
                        logging.info(str(tup))
                    readings.append(tup)
        logging.info(f"{len(readings)} neue Messwerte für Station {station} gefunden {t.read()}")
        return station, readings

    def _insert_readings(self, readings: list, c: Connection) -> None:
        with applog.Timer() as t:
            c.cur.executemany("""
                INSERT OR IGNORE INTO readings
                VALUES (?, ?,?,?,?, ?, ?,?)
            """, readings)
            # c.commit() -- commit außerhalb
        logging.info(f"{len(readings)} Zeilen in die Datenbank eingearbeitet {t.read()}")

    def _update_recent(self, readings: list, c: Connection) -> str:
        # get station, assuming that is the same in all tuples
        station = readings[0][0]
        # get max time of reading from last line
        # alternatively: https://stackoverflow.com/a/4800441/3991164
        r = readings[-1]
        yyyymmddhh = "%04d%02d%02d%02d" % (r[1], r[2], r[3], r[4])
        with applog.Timer() as t:
            # cf. https://stackoverflow.com/a/4330694/3991164
            c.cur.execute("""
                INSERT OR REPLACE
                INTO recent (station, yyyymmddhh)
                VALUES (?, ?)
            """, (station, yyyymmddhh))
            # c.commit() -- commit außerhalb
        logging.info(f"Neuester Messwert {yyyymmddhh} in der Datenbank vermerkt {t.read()}")
        return yyyymmddhh


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
