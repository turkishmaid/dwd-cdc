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

from time import perf_counter, process_time, sleep
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


GLOBAL_STAT = {
    "connection_sec": 0.0,
    "downloaded_bytes": 0,
    "download_time": 0.0,
    "files_cnt": 0,
    "readings_inserted": 0
}

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

    def __init__(self, text="some activities"):
        self._db = Path(config.get("databases", "folder")) / "hr-temp.sqlite"
        self.text = text

    def __enter__(self):
        """
        cur und con sind public für den Verwender
        """
        self.t0 = perf_counter()
        self.conn = sqlite3.connect(self._db)
        self.cur = self.conn.cursor()
        return self

    def commit(self):
        self.conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.cur = None
        self.conn = None
        dt = perf_counter() - self.t0
        GLOBAL_STAT["connection_sec"] += dt
        logging.info(f"Connection was open for {dt:.6f} s ({self.text})")


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
        GLOBAL_STAT["download_time"] += t.read(raw=True)
        GLOBAL_STAT["files_cnt"] += 1


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

        GLOBAL_STAT["downloaded_bytes"] += len(line)
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
            with Connection("insert stationen") as c:
                # https://database.guide/how-on-conflict-works-in-sqlite/
                c.cur.executemany("""
                    INSERT OR REPLACE INTO stationen
                    VALUES (?,?,?,?,?,?,?,?)
                """, self.rows)
                c.commit()
        logging.info(f"{self.cnt} Stationen in die Datenbank geschrieben {t.read()}")


LAND_MAP = {
    "Baden-Württemberg" : "BW",
    "Bayern" : "BY",
    "Berlin" : "BE",
    "Brandenburg" : "BB",
    "Bremen" : "HB",
    "Hamburg" : "HH",
    "Hessen" : "HE",
    "Mecklenburg-Vorpommern" : "MV",
    "Niedersachsen" : "NI",
    "Nordrhein-Westfalen" : "",
    "Rheinland-Pfalz" : "RP",
    "Saarland" : "SL",
    "Sachsen" : "SN",
    "Sachsen-Anhalt" : "ST",
    "Schleswig-Holstein" : "SH",
    "Thüringen" : "TH",
}

def station_name(station: int, c: Connection):
    sql = "select name, land from stationen where station = ?"
    c.cur.execute(sql, (station,))
    name = "?"
    land = "?"
    for row in c.cur:
        if row[0]:
            name = row[0]
            land = row[1]
    return f"{station}, {name} ({LAND_MAP[land]})"
#    return f"{station}, {name} ({land})"


def station_data_until(station: int, c: Connection):
    c.cur.execute("SELECT yyyymmddhh FROM recent where station = ?", (station,))
    until = NULLDATUM
    for row in c.cur:
        if row[0]:
            until = row[0]
    return until


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
        GLOBAL_STAT["download_time"] += t.read(raw=True)
        GLOBAL_STAT["files_cnt"] += 1

    def _collect(self, fnam: str) -> None:
        """
        Callback für FTP.retrlines.
        :param fnam: Zeile der Fileliste = Filename
        """
        self._zips.append(fnam)
        self._cnt += 1
        GLOBAL_STAT["downloaded_bytes"] += len(fnam)

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
        station, name, surpress_up_to = self._get_station(fnam)
        with applog.Timer() as t:
            with TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)
                logging.info(f"Temporäres Verzeichnis: {temp_dir}")
                zipfile_path = temp_dir / fnam
                self._download(ftp, fnam, zipfile_path)
                produkt_path = self._extract(zipfile_path, temp_dir)
                readings = self._parse(produkt_path, name, surpress_up_to)
                if readings:
                    with Connection("insert readings") as c:
                        self._insert_readings(readings, c)
                        last_date = self._update_recent(readings, c)
                        c.commit()  # gemeinsamer commit ist sinnvoll
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
        GLOBAL_STAT["downloaded_bytes"] += self._volume
        GLOBAL_STAT["download_time"] += t.read(raw=True)
        GLOBAL_STAT["files_cnt"] += 1

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

    def _get_station(self, fnam: str) -> tuple:
        """
        Bestimmt die Station aus dem Filenamen
        :param fnam: Filename einer .zip-Datei like "stundenwerte_TU_00003_19500401_20110331_hist.zip"
        :param c: Offene Connection zur Datenbank
        :return: tuple(Stationsnumer, druckbarer Name, Daten vorhanden bis)
        """
        with applog.Timer() as t:
            station = int(fnam.split(".")[0].split("_")[2])
            with Connection("get station detail") as c:
                with applog.Timer() as t:
                    name = station_name(station, c)
                    surpress_up_to = station_data_until(station, c)
        logging.info(f"Station {name} (Daten bis {surpress_up_to} bereits vorhanden) {t.read()}")
        return station, name, surpress_up_to

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

    # TODO prüfen, dass alle Werte auch von der gewünschten Station kommen
    def _parse(self, produkt_path: Path, name: str, surpress_up_to: str) -> list:
        """
        Parsen des Datenfiles.
        :param produkt_path: Pfad des Datenfiles
        :param name: druckbarer Name der Station
        :param surpress_up_to: Daten bis zu diesem Zeitpunkt sind bereits vorhanden, YYYYMMDDHH
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
        logging.info(f"{len(readings)} neue Messwerte für Station {name} gefunden {t.read()}")
        return readings

    def _insert_readings(self, readings: list, c: Connection) -> None:
        with applog.Timer() as t:
            c.cur.executemany("""
                INSERT OR IGNORE INTO readings
                VALUES (?, ?,?,?,?, ?, ?,?)
            """, readings)
            # c.commit() -- commit außerhalb
        logging.info(f"{len(readings)} Zeilen in die Datenbank eingearbeitet {t.read()}")
        GLOBAL_STAT["readings_inserted"] += len(readings)

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
    # for fnam in file_list[6:10]:
    for fnam in file_list:
        ProcessDataFile(ftp, fnam, verbose=True)
        logging.info("---")
        sleep(3)  # be nice: reduce load on server
    hurz = 17  # für Brechpunkt

    ftp.close()
    logging.info(f"Connection zum DWD geschlossen")

    logging.info("Statistik\n" + json.dumps(GLOBAL_STAT, indent=4))


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

            if args["--recent"]:
                process_dataset("recent")

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
