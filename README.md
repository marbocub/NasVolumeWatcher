# NasVolumeWatcher

Tools to store the hash of files on the NAS into the database, written in Python.

## Tools

This program contains two tools.

### volumefind.py

Find files on NAS volumes, caliculate their hashes and stores them into the database.

    usage:
        volumefind.py [-v] [-nf]
    options:
        -v    verbose mode
        -nf   no find mode

### volumewatch.py

Watch the creation/modification/deletion/movement of files on NAS volumes and store them into the database.

    usage:
        volumewatch.py [-v]
    options:
        -v    verbose mode

## Prerequirements

* Python >= 3.5
* PostgreSQL

## Configurations

Create the .env file and edit them for the your own database.

    $ cp .env.example .env

## License

Copyright (c) 2021 @marbocub marbocub@gmail.com, All rights reserved.

This program is released under the MIT License -
see the [LICENSE](LICENSE) file for details.
