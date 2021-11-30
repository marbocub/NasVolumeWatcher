# NasVolumeWatcher

Tools to store the hash of files on the NAS into the database, written in Python.

## Prerequirements

* Python >= 3.5
* PostgreSQL
* python-dotenv library
* psycopg (>=3.0) library
* watchdog library

You can install python libraries by using the pip.

    python3 -m pip install python-dotenv psycopg watchdog

## Environment Configuration

This program uses the python-dotenv library and it is easy to configure using the file named ".env" for your environment.
The program contains a file named ".env.example" is a template, so you can copy to ".env" and edit it.

    $ cp .env.example .env

### database configuration

DB_* are environment variables that configure the database.

* DB_HOST=hostname or IP address for the database (e.g. 127.0.0.1)
* DB_PORT=port number of the database (e.g. 5432)
* DB_DATABASE=name of database (e.g. filehashes)
* DB_USERNAME=username of database
* DB_PASSWORD=password of database user

### target directories and deny list

VOLUMES is an environment variable that sets the target volumes.

* VOLUMES='/volume1'

DENYLIST is an environment variable that sets the top-level directory to excludes from finding of volumes. The directory names matchs by prefix matching.

* DENYLIST='@,report'

You can configure multiple items separated by a comma for VOLUMES and DENYLIST.

## Tools

This program contains two tools.

### volumefind.py

Find files on NAS volumes, caliculate their hashes and stores them into the database.

    usage:
        volumefind.py [-v] [-nf]
    options:
        -v    verbose mode
        -nf   no find mode

If you use Synology's NAS, we recommend setup two tasks below.

* python3 yourpath/volumefind.py -nf : Every 5 munites.
* python3 yourpath/volumefind.py : Every 1 hour.

### volumewatch.py

Watch the creation/modification/deletion/movement of files on NAS volumes and store them into the database.
This program works permanently if no errors occurs.

    usage:
        volumewatch.py [-v]
    options:
        -v    verbose mode

If you use Synology's NAS, we recommend setup two tasks below.

* python3 yourpath/volumewatch.py : Every 1 hour or start by manual.

## License

Copyright (c) 2021 @marbocub marbocub@gmail.com, All rights reserved.

This program is released under the MIT License -
see the [LICENSE](LICENSE) file for details.
