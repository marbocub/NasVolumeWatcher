#!/usr/bin/env python3
#
# Copyright (c) 2021 @marbocub <marbocub@gmail.com>
# Released under the MIT license
#

import os, pathlib, hashlib, time, sys, re
from typing import Callable, List, Tuple
import dotenv

from database import FileBase, File, Dir, DatabaseInterface, DatabasePostgreSQL

#----------------------------------------------------------------------
# calc hash
#----------------------------------------------------------------------
def sha256sum(path: str) -> str:
    try:
        sha256 = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096 * sha256.block_size), b''):
                sha256.update(chunk)
    except Exception as e:
        print("exception: {0}".format(e))
        return None
    return sha256.hexdigest()

#----------------------------------------------------------------------
# search files and calc hashes
#----------------------------------------------------------------------
class FileFinder:
    database: DatabaseInterface = None
    logger: Callable = None

    def __init__(self, database: DatabaseInterface = None, logger: Callable = None) -> None:
        self.database = database
        self.logger = logger or (lambda *x, **y: None)

    def _find_progress(self, topdir: str, childdirs: list, childfiles: list, end: str = "\r") -> None:
        self.logger("  {0}: {1} files, {2} dirs".format(topdir, len(childfiles), len(childdirs)), end=end)

    def find(self, volumes: List[str] = ['/volume1'], denylist: List[str] = []) -> Tuple[List[File], List[Dir]]:
        files = []
        dirs = []
        cwd = os.getcwd()
        for volume in volumes:
            os.chdir(volume)
            self.logger(volume)
            for topdir in os.listdir():
                for wanted in denylist:
                    if topdir[:len(wanted)] == wanted:
                        break
                else:
                    dirs.append(Dir(volume = volume, path = topdir, stat = os.stat(topdir)))
                    childdirs = []
                    childfiles = []
                    self._find_progress(topdir, childdirs, childfiles)
                    count = 0
                    for file in pathlib.Path(topdir).glob("**/*"):
                        if re.search('@eaDir', str(file)):
                            continue
                        if file.is_symlink() or not (file.is_file() or file.is_dir()):
                            continue
                        count += 1
                        if file.is_file():
                            childfiles.append(File(
                                volume = volume,
                                path = str(file),
                                parent = str(file.parent),
                                stat = file.stat(),
                            ))
                        elif file.is_dir():
                            childdirs.append(Dir(
                                volume = volume,
                                path = str(file),
                                parent = str(file.parent),
                                stat = file.stat(),
                            ))
                        if count % 100 == 0:
                            self._find_progress(topdir, childdirs, childfiles)
                    self._find_progress(topdir, childdirs, childfiles, end="\n")
                    dirs += childdirs
                    files += childfiles
        os.chdir(cwd)
        self.logger("  -- Total: files={0}, dirs={1}".format(len(files), len(dirs)))
        self.database.files = tuple(files)
        self.database.dirs = tuple(dirs)

        return (files, dirs)

    def _calc_hash_progress(self, hashed: int, skip: int, saved: int, end: str = "\n"):
        self.logger("  {0} files hashed, {1} files skiped, {2} hashes saved".format(hashed, skip, saved), end=end)

    def calc_hash(self) -> int:
        hashed: int = 0
        saved: int = 0
        skip: int = 0
        while True:
            modified: List[FileBase] = self.database.modified
            if len(modified) == 0:
                break
            files: List[FileBase] = []
            start = time.time()
            for file in modified:
                fullpath = os.path.join(file.volume, file.path)
                if os.path.exists(fullpath):
                    file.stat = os.stat(fullpath)
                    file.sha256 = sha256sum(fullpath)
                    file.rehash = None
                    files.append(file)
                    hashed += 1
                else:
                    self.database.deleted = (file,)
                    skip += 1

                if (time.time() - start) > 6:
                    self.database.updated = tuple(files)
                    saved += len(files)
                    files = []
                    start = time.time()
                self._calc_hash_progress(hashed, skip, saved, end="\r")
            self.database.updated = tuple(files)
            saved += len(files)
        self._calc_hash_progress(hashed, skip, saved)
        self.logger("  updating counter...")
        self.database.updated = ()
        self.logger("  done.")
        return saved

#----------------------------------------------------------------------
# main
#----------------------------------------------------------------------
def usage():
    print('usage:')
    print('  {0} [-v] [-nf]'.format(sys.argv[0]))
    print('options:')
    print('  -v    verbose mode')
    print('  -nf   no find mode')

def getopt():
    verbose = False
    findfiles = True
    for a in sys.argv[1:]:
        if a == "-v":
            verbose = True
        elif a == "-nf":
            findfiles = False
        else:
            usage()
            sys.exit()
    return (verbose, findfiles)

def main():
    start = time.time()
    (verbose, findfiles) = getopt()
    logger = print if verbose else (lambda *x, **y: None)

    logger("initializing...")
    dotenv.load_dotenv()
    volumes  = list(filter(lambda x: x!='', (os.environ.get('VOLUMES')  or "/volume1").split(',')))
    denylist = list(filter(lambda x: x!='', (os.environ.get('DENYLIST') or "").split(',')))

    logger("connecting database...")
    database = DatabasePostgreSQL(logger=logger)
    filefinder = FileFinder(database = database, logger=logger)

    logger("done: {0}".format(time.time()-start))

    if findfiles:
        logger("---find files---")
        filefinder.find(volumes, denylist)
        logger("done: {0}".format(time.time()-start))

    logger("---calc hash---")
    filefinder.calc_hash()
    logger("done: {0}".format(time.time()-start))

    del database
    logger("---done---")

if __name__ == '__main__':
    main()
