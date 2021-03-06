# dwd-cdc

Fool around with data from DWD Climate Data Center.

### Failed Development Plan

This does not work out as intended, because I am using JetBrain's PyCharm for development. So either I cannot structure my code properly, or I will have to do without PyCharm autocomplete fuctionality. So I will need to choose a structure that is supported by both Python and PyCharm. We will see what I can find out. See you later.

----

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
- clone repository from GitHub into local workspace or temp directory
- execute sevral scripts programatically
   - in the repo root folder 
   - leaving logs behind
- exit (and optionally remove workspace or temp directory)

We thus develop for an ever refreshed git workspace in an
execution environment and not an installable PyPI module or so.

### Mailgun-Anschluss (optional)

Nachdem die Konfigurationsdatei `~/.dwd-cdc/dwd-cdc` angelegt ist, 
kann dort ein Abschnitt wie folgt manuell hinzugefügt werden:

```ini
[mailgun]
url = https://api.mailgun.net/v3/sandbox12345678901234567890123.mailgun.org/messages
auth-key = key-8674f976bb0w8678a0ds874sjldao787
from = dwd-cdc <postmaster@sandbox12345678901234567890123.mailgun.org>
to = Sara Ziner <do.not.use@example.com>
```

Wenn diese Konfiguration vorhanden ist, wird nach jedem Programmlauf die Print- und Log-Ausgabe über den
beschriebenen Mailgun-Account an die angegebene `to`-Adresse geschickt. Weitergehende Konfigurationsmöglichkeiten
werden (vielleicht) später hinzugefügt.

Ein kostenloser [mailgun](https://www.mailgun.com/) Account ("Flex Trial") ist für die Verwendung hier völlig
ausreichend. Man muss allerdings die Empfängeradressen vorher als "authorized recipients" anmelden 
und diese müssen es auch bestätigen.


 
