#!/usr/bin/env python
# coding: utf-8

"""
Download DWD Stundenwerte Lufttemperatur 2m.

Usage:
  hr-temp.py ( --recent | --historical | --stations | --test )

Options:
  -h --help         Zeige die Bedeutung der Parameter
  --recent          Download der letzten ca. 500 Tage
  --historical      Download des historischen Datenbestandes (das setzt eine
                    leere Datenbank voraus, prüft es aber nicht)
  --stations        Download der Stationsliste
  --test            Experimentellen oder Einmal-Code ausführen
"""
# Created: 09.08.20

# TODO Möglichkeit, die Stationen zu limitieren, etwa in Form einer übersteuerbaren Liste in der .ini-Datei
# TODO Prüffunktion, um einen Vollständigkeitsappell für die Messwerte zu machen (Zeilen und -999)

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
from dataclasses import dataclass
from datetime import date, timedelta

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
        # DONE sqlite3.OperationalError: database is locked – der Leseversuch wird von außen wiederholt
        # TODO Retry in die Connection-Klasse einbauen statt im Aufrufer

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

    # DONE im aufrufer: absichern mit try und sleep/retry, kein Programmabbruch bei Fehler
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
    "Baden-Württemberg": "BW",
    "Bayern": "BY",
    "Berlin": "BE",
    "Brandenburg": "BB",
    "Bremen": "HB",
    "Hamburg": "HH",
    "Hessen": "HE",
    "Mecklenburg-Vorpommern": "MV",
    "Niedersachsen": "NI",
    "Nordrhein-Westfalen": "",
    "Rheinland-Pfalz": "RP",
    "Saarland": "SL",
    "Sachsen": "SN",
    "Sachsen-Anhalt": "ST",
    "Schleswig-Holstein": "SH",
    "Thüringen": "TH",
    "?": "?"
}

@dataclass
class Station:
    station: int
    name: str
    land: str
    isodate_von: str
    isodate_bis: str
    dwdts_recent: str       # last known as in recent table
    dwdts_readings: str     # last known as in readings table
    description: str
    populated: bool = False

    def __init__(self, station):
        # select < 0.6 millis :)
        sql = """select 
                name, land, yyyymmdd_von, yyyymmdd_bis, 
                ifnull(rc.yyyymmddhh, '1700010100'),
                ifnull(max(rd.dwdts), '1700010100') 
            from stationen s
            left outer join recent rc on s.station = rc.station
            left join readings rd on s.station = rd.station
            where s.station = ?"""

        self.station = station
        with applog.Timer() as t:
            with Connection(f"Station.__init__({station})") as c:
                with applog.Timer() as t:
                    c.cur.execute(sql, (station,))
                    row = c.cur.fetchone()
                    if row:
                        self.name = row[0]
                        self.land = row[1]
                        self.isodate_von = row[2]
                        self.isodate_bis = row[3]
                        self.dwdts_recent = row[4]  # aus Tabelle
                        self.dwdts_readings = row[5]  # aus Daten
                        assert self.dwdts_recent == self.dwdts_readings, \
                            f"recent: {self.dwdts_recent} vs. Daten: {self.dwdts_readings}"
                        self.populated = True
                        self.description = f"{self.station}, {self.name} ({LAND_MAP[self.land]})"
                        logging.info(f"{self.description}: {self.isodate_von}..{self.isodate_bis} "
                                     f"rc={self.dwdts_recent} rd={self.dwdts_readings}")
                    else:
                        self.populated = False


def is_data_expected(fnam: str = None, s: Station = None) -> bool:
    """
    :param fnam: filename like stundenwerte_TU_05146_20040601_20191231_hist.zip or stundenwerte_TU_01072_akt.zip
    :param s: (optional) Station object for the station in question, used when supplied
    :return: True when the file is assumed to contain new values
    """
    station = int(fnam.split(".")[0].split("_")[2])
    if s:
        assert s.station == station, f"Station aus Filename: {station}, aus Objekt: {s.station}"
    else:
        s = Station(station)
    assert s.populated
    if fnam.endswith("_hist.zip"):
        bis = fnam.split(".")[0].split("_")[4] + "23"
        if s.dwdts_readings > "1701" and bis < "2018":
            # es gibt bereits Werte und die Station sendet schon länger nicht mehr
            return False
        return s.dwdts_readings < bis
    else:
        assert fnam.endswith("_akt.zip"), fnam
        # we do not have to care for time zone here, b/c DWD will typically upload yesterdays data between 9am and 10am local time
        yester_dwdts = (date.today() + timedelta(days=-1)).strftime("%Y%m%d") + "23"
        # Vereinfachung: Wenn Daten bis gestern schon da sind -> nix tun
        # TODO Wenn Daten bis zum Ende der Station schon da sind -> nix tun
        return s.dwdts_readings < yester_dwdts


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
        self.did_download = False
        logging.info(f'DataFile(_,"{fnam}")')

        station_nr = int(fnam.split(".")[0].split("_")[2])  # geht erfreulicherweise für hist und akt
        self.station = Station(station_nr)
        logging.info(f"Station {self.station.description} (Daten bis {self.station.dwdts_recent} bereits vorhanden)")
        if is_data_expected(fnam, self.station):
            with applog.Timer() as t:
                with TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    logging.info(f"Temporäres Verzeichnis: {temp_dir}")
                    zipfile_path = temp_dir / fnam
                    self._download(ftp, fnam, zipfile_path)
                    produkt_path = self._extract(zipfile_path, temp_dir)
                    readings = self._parse(produkt_path)
                    if readings:
                        with Connection("insert readings") as c:
                            self._insert_readings(readings, c)
                            last_date = self._update_recent(readings, c)
                            c.commit()  # gemeinsamer commit ist sinnvoll
                        logging.info(f"Werte für Station {self.station.description} bis {last_date} verarbeitet {t.read()}")
                    else:
                        logging.info(f"Keine Werte für Station {self.station.description} nach {self.station.dwdts_recent} gefunden {t.read()}")
            if temp_dir.exists():
                applog.ERROR = True
                logging.error(f"Temporäres Verzeichnis {temp_dir} wurde NICHT entfernt")
        else:
            logging.info(f"File {fnam} wird nicht heruntergeladen, da keine neuen Daten zu erwarten sind.")

    def _download(self, ftp: FTP, from_fnam: str, to_path: Path) -> None:
        """
        :param ftp: geöffnete FTP Verbindung mit dem richtigen Arbeitsverzeichnis
        :param from_fnam: Name des herunterzuladenden Files
        :param to_path: Speicherort lokal
        """
        self._cnt = 0
        self._volume = 0
        # DONE im Aufrufer: absichern mit try und sleep/retry, kein Programmabbruch bei Fehler
        with applog.Timer() as t:
            with open(to_path, 'wb') as self.open_zip_file:
                ftp.retrbinary("RETR " + from_fnam, self._collect)
                # TODO TimeoutError: [Errno 60] Operation timed out
                self.did_download = True
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
    def _parse(self, produkt_path: Path) -> list:
        """
        Parsen des Datenfiles. Für die Feststellung zu unterdrückender Zeilen wird self.station benutzt
        :param produkt_path: Pfad des Datenfiles
        :return: eine Liste von Tupeln, die in die Tabelle readings eingefügt werden können
        """

        def ymdh(yymmddhh: str) -> tuple:
            """
            Aufbrechen der DWD Zeitangabe in numerische Zeiteinheiten.
            :param yymmddhh: Stunde in DWD-Format
            :return: Tuple mit den numerischen Teilkomponenten
            """
            y = int(yymmddhh[:4])
            m = int(yymmddhh[4:6])
            d = int(yymmddhh[6:8])
            h = int(yymmddhh[-2:])
            return y, m, d, h

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
                        continue
                    # surpress data that might be in DB already
                    if row[1] <= self.station.dwdts_recent:
                        continue
                    elif skipped == -1:  # now uncond.
                        skipped = cnt - 2  # current and first excluded
                        logging.info(f"{skipped} Messwerte vor dem {self.station.dwdts_recent} wurden übersprungen")
                    if shown <= 1:  # show first 2 rows taken
                        shown += 1
                        logging.info(f"{row[0]}, {row[1]}")
                    y, m, d, h = ymdh(row[1])
                    tup = (
                        int(row[0]),  # station
                        row[1],
                        y, m, d, h,  # row[1],
                        int(row[2]),  # q
                        None if row[3].strip() == "-999" else float(row[3]),  # temp
                        None if row[4].strip() == "-999" else float(row[4])  # humid
                    )
                    readings.append(tup)
        logging.info(f"{len(readings)} neue Messwerte für Station {self.station.description} gefunden {t.read()}")
        return readings

    def _insert_readings(self, readings: list, c: Connection) -> None:
        with applog.Timer() as t:
            c.cur.executemany("""
                INSERT OR IGNORE INTO readings
                VALUES (?, ?,?,?,?,?, ?, ?,?)
            """, readings)
            # c.commit() -- commit außerhalb
        logging.info(f"{len(readings)} Zeilen in die Datenbank eingearbeitet {t.read()}")
        GLOBAL_STAT["readings_inserted"] += len(readings)

    def _update_recent(self, readings: list, c: Connection) -> str:
        # get station, assuming that is the same in all tuples
        station = readings[0][0]
        # get max time of reading from last line
        # alternatively: https://stackoverflow.com/a/4800441/3991164
        yyyymmddhh = readings[-1][1]
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

    for attempt in range(3):
        try:
            file_list = FtpFileList(ftp).get()
        except TimeoutError as ex:  # will hopefully catch socket timeout
            logging.exception("Timeout - " + ("will retry" if attempt < 2 else "no more retries"))
            sleep(3)  # give the server a break
        else:
            break
    else:
        applog.ERROR = True
        raise RuntimeError("Finally failed to retrieve list of zip-files - terminating...")

    # for fnam in file_list[6:10]:
    for i, fnam in enumerate(file_list):
        # https://stackoverflow.com/a/7663441/3991164 - resiliance is hard to test...
        # TODO Wenn der Fehler aufgrund Datenbank-Lock kommt, wird hier überflüssig das File nochmal runtergeladen
        # DONE nur aufgrund eigener Daten und fnam prüfen, ob die Daten von gestern schon da sind (oder die Station eh keine Daten mehr liefert) und in diesem Fall überspringen. Gut für Retries bei Fehlern.
        for attempt in range(3):
            try:
                p = ProcessDataFile(ftp, fnam, verbose=True)
                logging.info(f"--- {i/len(file_list)*100:.0f} %")
                if p.did_download:  # be nice: reduce load on server
                    sleep(3)
            except TimeoutError as ex:  # will hopefully catch socket timeout
                logging.exception("Timeout - " + ("will retry" if attempt < 2 else "no more retries"))
                # TODO Pause über .ini File konfigurierbar machen
                sleep(3)  # give the server a break
            except Exception as ex:  # will not catch KeyboardInterrupt :)
                logging.exception("Exception caught - " + ("will retry" if attempt < 2 else "no more retries"))
                sleep(3)  # give it a break
            else:  # executed when the execution falls thru the try
                break
        else:
            applog.ERROR = True
            logging.error(f"Finally failed to process file {fnam}")
            # continues to next file

    hurz = 17  # für Brechpunkt

    ftp.close()
    logging.info(f"Connection zum DWD geschlossen")

    logging.info("Statistik\n" + json.dumps(GLOBAL_STAT, indent=4))


def experimental():
    pass


OPCODE = None     # enable one for interactive debugging in IDE w/o using run configurations
# OPCODE = "test"
# OPCODE = "recent"
# OPCODE = "historical"
# OPCODE = "stations"

if __name__ == "__main__":
    tracemalloc.start()
    pc0 = perf_counter()
    pt0 = process_time()
    dotfolder.ensure_dotfolder()
    applog.init_logging(collective=True, console=True, process=True)
    dotfolder.init_dotfolder()
    logging.info("Willkommen beim DWD.")
    try:
        try:
            # support interactive debugging
            if OPCODE:
                args = {
                    "--recent": OPCODE == "recent",
                    "--historical": OPCODE == "historical",
                    "--stations": OPCODE == "stations",
                    "--test": OPCODE == "test"
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
                # TODO nach dem Runterladen eine Kopie der Datenbank für Auswertungszwecke machen

            if args["--test"]:
                experimental()

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
        logging.warning("caught KeyboardInterrupt")
    except Exception as ex:
        applog.ERROR = True
        logging.exception("Sorry.")
    applog.shoot_mail("Stundenwerte Lufttemperatur 2m - DWD-CDC Download")
    logging.info("Ciao.")
    print()
    print()
