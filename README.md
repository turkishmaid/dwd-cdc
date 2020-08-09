# dwd-cdc

Fool around with data from DWD Climate Data Center.

## Purpose

Download and update data from the DWD Climata Data Center
into a local database to support future analysis, e.g. from
automatic scripts (this repository) or Jupyter notebooks
(other repository).

## Development Pattern

This project is built for Jenkins (or cron) based usage. 
This means that the content of this repository will 
periodically be executed from a Jenkins instance or via cron
according to the following pattern:
- clone repository from GutHub into local workspace or temp directory
- execute sevral scripts programatically
   - in the repo root folder, 
    - leaving logs behind
- exit (and optionally remove workspace or temp directory)

We thus develop for an ever refreshed git workspace in an
execution environment and not an installable PyPI module or so.


 
