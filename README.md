# NasVolumeWatcher

Tools to build the hash database for existed files on the NAS, written in Python.

## Prerequirements

* Python >= 3.5
* PostgreSQL
* python-dotenv library
* psycopg (>=3.0) library
* watchdog library

You can install python libraries by using the pip.

    python3 -m pip install python-dotenv psycopg watchdog

## Environment Configuration

This program uses the python-dotenv library and it easy to configure using the file named ".env" for your environment.
The program contains a file named ".env.example" is a template, so you can copy to ".env" and edit it.

    $ cp .env.example .env

### database configuration

DB_* are environment variables that configure the database.

* DB_HOST=hostname or IP address for the database server (e.g. 127.0.0.1)
* DB_PORT=port number of the database server (e.g. 5432)
* DB_DATABASE=name of the database (e.g. "filehashes")
* DB_USERNAME=username of the database user
* DB_PASSWORD=password of the database user

The database and the user must be created before using this program.

### target volumes and deny directories list

VOLUMES is an environment variable that sets the target volumes.

* VOLUMES='/volume1'

DENYLIST is an environment variable that sets the top-level directory to excludes from finding of volumes.
The directory names matchs by prefix matching.

* DENYLIST='@,report'

You can configure multiple items separated by a comma for VOLUMES and DENYLIST.

## Tools

This program contains two tools.

### volumefind.py

This tool find all existed files and directories on the targeted NAS volumes excluding directories in the deny list,
calculate their hashes if needed and stores them into the database.

    usage:
        volumefind.py [-v] [-nf]
    options:
        -v    verbose mode
        -nf   no find mode

If you use Synology's NAS, we recommend setup two tasks below.

* python3 yourpath/volumefind.py -nf : Every 5 minutes. (hash calculateing only)
* python3 yourpath/volumefind.py : Every 1 hour. (finding files and hash calculating)

### volumewatch.py

This tool watches the creation / modification / deletion / movement of files or dirs on the NAS volumes and updates the database.
When a file is created or modified, (re-)calculating the hash is needed, however, hash calculation takes a long time.
Thus, this tool not set the hash column in the both case and set the "rehash" flag of the "files" table in the case of a file is modified.

    usage:
        volumewatch.py [-v]
    options:
        -v    verbose mode

This tool works permanently if no errors occurs.
If you use Synology's NAS, we recommend setup a task below.

* python3 yourpath/volumewatch.py : Every 1 hour or start by manual.

The volumefind.py selects rows that needed to be (re-)hashed, calculates the hashes and updates the rows.
So you can simply quickly update hashes by combining volumewatch.py and volumefind.py.

## License

Copyright (c) 2021 @marbocub marbocub@gmail.com, All rights reserved.

This program is released under the MIT License -
see the [LICENSE](LICENSE) file for details.
