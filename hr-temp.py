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

import json
import os
from time import sleep
import logging
from ftplib import FTP
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile
import csv
from dataclasses import dataclass
from datetime import date, timedelta

from docopt import docopt

import johanna
import ftplight


def iso_date(s: str) -> str:
    """
    Formatiert ds CDC Datum als ISO-Datum. Bsp.: 19690523 -> 1969-05-23
    :param s: Datum in Schreibweise YYYYMMDD
    :return: Datum in Schreibweise YYYY-MM-DD
    """
    # 19690523 -> 1969-05-23
    return "%s-%s-%s" % (s[0:4], s[4:6], s[6:])


class ProcessStationen:
    # ein File, alle Stationen (Stammdaten), kapselt den Ursprungsort der Liste

    def __init__(self):
        self._download()
        self._upsert()

    # DONE im aufrufer: absichern mit try und sleep/retry, kein Programmabbruch bei Fehler
    def _download(self) -> None:
        with johanna.Timer() as t:
            ftp = ftplight.dwd("climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical")
            fnam = "TU_Stundenwerte_Beschreibung_Stationen.txt"
            self.lines = ftplight.ftp_retrlines(ftp, from_fnam=fnam, verbose=True)
            self.rows = []
            self.cnt = 0
            for line in self.lines:
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
                        int(parts[0]),  # station integer,
                        iso_date(parts[1]),  # yymmdd_von text,
                        iso_date(parts[2]),  # yymmdd_bis text,
                        int(parts[3]),  # hoehe integer,
                        float(parts[4]),  # breite real,
                        float(parts[5]),  # laenge real,
                        " ".join(parts[6:-1]),  # name text,
                        parts[-1]  # (bundes)land text
                    )
                    self.rows.append(tup)
                    self.cnt += 1
            logging.info(f"{self.cnt} Stationen gelesen und geparst {t.read()}")
            ftp.quit()
        logging.info(f"Verbindung zum DWD geschlossen {t.read()}")
        johanna.collect_stat("download_sec", t.read(raw=True))
        johanna.collect_stat("files_cnt", 1)

    def _upsert(self):
        with johanna.Timer() as t:
            with johanna.Connection("insert stationen") as c:
                # https://database.guide/how-on-conflict-works-in-sqlite/
                c.cur.executemany("""
                    INSERT OR REPLACE INTO stationen
                    VALUES (?,?,?,?,?,?,?,?)
                """, self.rows)
                c.commit()
        logging.info(f"{self.cnt} Stationen in die Datenbank geschrieben {t.read()}")


# https://www.giga.de/ratgeber/specials/abkuerzungen-der-bundeslaender-in-deutschland-tabelle/
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
        with johanna.Timer() as t:
            with johanna.Connection(f"Station.__init__({station})") as c:
                with johanna.Timer() as t:
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
            with johanna.Timer() as t:
                with TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    logging.info(f"Temporäres Verzeichnis: {temp_dir}")
                    zipfile_path = ftplight.ftp_retr(ftp, from_fnam=fnam, to_path=temp_dir/fnam, verbose=True)
                    if not zipfile_path:
                        johanna.flag_as_error()
                        logging.error(f"Kann die Daten der Station {self.station.description} nicht herunterladen.")
                        return
                    produkt_path = self._extract(zipfile_path, temp_dir)
                    readings = self._parse(produkt_path)
                    if readings:
                        # TODO connection mit retry absichern
                        with johanna.Connection("insert readings") as c:
                            self._insert_readings(readings, c)
                            last_date = self._update_recent(readings, c)
                            c.commit()  # gemeinsamer commit ist sinnvoll
                        logging.info(f"Werte für Station {self.station.description} bis {last_date} verarbeitet {t.read()}")
                    else:
                        logging.info(f"Keine Werte für Station {self.station.description} nach {self.station.dwdts_recent} gefunden {t.read()}")
            if temp_dir.exists():
                johanna.flag_as_error()
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
        with johanna.Timer() as t:
            with open(to_path, 'wb') as self.open_zip_file:
                ftp.retrbinary("RETR " + from_fnam, self._collect)
                # TODO TimeoutError: [Errno 60] Operation timed out
                self.did_download = True
            if self._verbose:
                print()  # awkward
        logging.info(f"Zipfile heruntergeladen: {self._volume:,} Bytes in {self._cnt} Blöcken {t.read()}")
        johanna.collect_stat("download_bytes", self._volume)
        johanna.collect_stat("download_time", t.read(raw=True))
        johanna.collect_stat("files_cnt", 1)

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

        with johanna.Timer() as t:
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

    def _insert_readings(self, readings: list, c: johanna.Connection) -> None:
        with johanna.Timer() as t:
            c.cur.executemany("""
                INSERT OR IGNORE INTO readings
                VALUES (?, ?,?,?,?,?, ?, ?,?)
            """, readings)
            # c.commit() -- commit außerhalb
        logging.info(f"{len(readings)} Zeilen in die Datenbank eingearbeitet {t.read()}")
        johanna.collect_stat("readings_inserted", len(readings))

    def _update_recent(self, readings: list, c: johanna.Connection) -> str:
        # get station, assuming that is the same in all tuples
        station = readings[0][0]
        # get max time of reading from last line
        # alternatively: https://stackoverflow.com/a/4800441/3991164
        yyyymmddhh = readings[-1][1]
        with johanna.Timer() as t:
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
    with johanna.Timer() as t:
        ftp = FTP(SERVER)
        ftp.login()  # anonymous
        ftp.cwd(remote)
    logging.info(f"Connectiert an {SERVER} pwd={remote} {t.read()}")

    file_list = ftplight.ftp_nlst(ftp)
    if not file_list:
        raise Exception("Da kann ich nix machen.")

    for i, fnam in enumerate(file_list):
        # https://stackoverflow.com/a/7663441/3991164 - resiliance is hard to test...
        p = ProcessDataFile(ftp, fnam, verbose=True)
        logging.info(f"--- {i/len(file_list)*100:.0f} %")

    hurz = 17  # für Brechpunkt

    ftp.close()
    logging.info(f"Connection zum DWD geschlossen")

    #logging.info("Statistik\n" + json.dumps(GLOBAL_STAT, indent=4))


def experimental():
    pass


OPCODE = None     # enable one for interactive debugging in IDE w/o using run configurations
# OPCODE = "test"
# OPCODE = "recent"
# OPCODE = "historical"
# OPCODE = "stations"

def main():
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


if __name__ == "__main__":
    johanna.main(callback=main,
                 mail_subject="Stundenwerte Lufttemperatur 2m - DWD-CDC Download",
                 dotfolder=Path(os.environ["HOME"]) / ".dwd-cdc",
                 dbname="hr-temp.sqlite")
