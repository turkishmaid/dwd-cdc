-- noinspection SqlNoDataSourceInspectionForFile

-- Data source: hourly air-temperature-2m
-- http://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/
-- schema version 00
-- updates will get names like hr-temp-01.sql

CREATE TABLE IF NOT EXISTS readings (
    station INTEGER,
    -- date TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    qn9 INTEGER,
    temp REAL,
    humid REAL
);

CREATE TABLE IF NOT EXISTS recent (
    station INTEGER PRIMARY KEY,
    yyyymmddhh TEXT       -- maximum in DB
);

-- http://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt

CREATE TABLE stationen (
    station INTEGER,
    yyyymmdd_von TEXT,
    yyyymmdd_bis TEXT,
    hoehe INTEGER,
    breite REAL,
    laenge REAL,
    name TEXT,
    land TEXT
);
