#!/usr/bin/env python3
#
# Copyright (c) 2021 @marbocub <marbocub@gmail.com>
# Released under the MIT license
#

import os, datetime, time, sys, re
from typing import Callable, List, Dict, Tuple

import dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import DirCreatedEvent, FileCreatedEvent
from watchdog.events import DirDeletedEvent, FileDeletedEvent
from watchdog.events import DirModifiedEvent, FileModifiedEvent
from watchdog.events import DirMovedEvent, FileMovedEvent

from database import FileBase, File, Dir, DatabaseInterface, DatabasePostgreSQL

class WatchEventHandler(FileSystemEventHandler):
    volume: str = None
    database: DatabaseInterface = None
    logger: Callable = None

    def __init__(self, volume: str = None, database: DatabaseInterface = None, logger: Callable = None):
        self.volume = volume
        self.database = database
        self.logger = logger or (lambda *x, **y: None)

    def on_created(self, event):
        if re.search('@eaDir', str(event.src_path)):
            return
        fullpath = os.path.join(self.volume, event.src_path)
        if not os.path.exists(fullpath):
            return
        if os.path.islink(fullpath):
            return
        self.logger("created:", event.src_path)
        try:
            stat = os.stat(fullpath)
            parent = os.path.dirname(event.src_path)
            if isinstance(event, DirCreatedEvent):
                file = Dir(volume = self.volume, path = event.src_path, parent = parent, stat = stat)
            elif isinstance(event, FileCreatedEvent):
                file = File(volume = self.volume, path = event.src_path, parent = parent, stat = stat)
            else:
                self.logger(type(event))
                return
            self.database.created = (file,)
        except Exception as e:
            self.logger("catch: {}".format(e))

    def on_deleted(self, event):
        if re.search('@eaDir', str(event.src_path)):
            return
        self.logger("deleted:", event.src_path)
        try:
            if isinstance(event, DirDeletedEvent):
                file = Dir(volume=self.volume, path=event.src_path)
            elif isinstance(event, FileDeletedEvent):
                file = File(volume=self.volume, path=event.src_path)
            else:
                self.logger(type(event))
                return
            self.database.deleted = (file,)
        except Exception as e:
            self.logger("catch: {}".format(e))

    def on_modified(self, event):
        if re.search('@eaDir', str(event.src_path)):
            return
        self.logger("modified:", event.src_path)
        try:
            if isinstance(event, DirModifiedEvent):
                file = Dir(volume=self.volume, path=event.src_path)
            elif isinstance(event, FileModifiedEvent):
                file = File(volume=self.volume, path=event.src_path)
            else:
                self.logger(type(event))
                return
            self.database.modified = (file,)
        except Exception as e:
            self.logger("catch: {}".format(e))

    def on_moved(self, event):
        if re.search('@eaDir', str(event.src_path)):
            return
        self.logger("moved: {} -> {}".format(event.src_path, event.dest_path))
        try:
            dest_parent = os.path.dirname(event.dest_path)
            if isinstance(event, DirMovedEvent):
                src = Dir(volume=self.volume, path=event.src_path)
                dst = Dir(volume=self.volume, path=event.dest_path, parent=dest_parent)
            elif isinstance(event, FileMovedEvent):
                src = File(volume=self.volume, path=event.src_path)
                dst = File(volume=self.volume, path=event.dest_path, parent=dest_parent)
            else:
                self.logger(type(event))
                return
            self.database.moved = {src: dst}
        except Exception as e:
            self.logger("catch: {}".format(e))

#----------------------------------------------------------------------
# main
#----------------------------------------------------------------------
def usage():
    print('usage:')
    print('  {0} [-v]'.format(sys.argv[0]))
    print('options:')
    print('  -v    verbose mode')

def getopt():
    verbose = False
    for a in sys.argv[1:]:
        if a == "-v":
            verbose = True
        else:
            usage()
            sys.exit()
    return (verbose,)

def main():
    (verbose,) = getopt()
    logger = print if verbose else (lambda *x, **y: None)

    logger("initializing...")
    dotenv.load_dotenv()
    volumes  = list(filter(lambda x: x!='', (os.environ.get('VOLUMES')  or "/volume1").split(',')))
    denylist = list(filter(lambda x: x!='', (os.environ.get('DENYLIST') or "").split(',')))

    logger("connecting database...")
    database = DatabasePostgreSQL(logger=logger)

    logger("setting up watchdog...")
    threads = []
    handlers = []
    observer = Observer()
    for volume in volumes:
        os.chdir(volume)
        event_handler = WatchEventHandler(volume, database=database, logger=logger)
        for dir in os.listdir():
            for wanted in denylist:
                if dir[:len(wanted)] == wanted:
                    break
            else:
                logger("  ", volume, dir)
                observer.schedule(event_handler, path=dir, recursive=True)
                threads.append(observer)
        handlers.append(event_handler)
    observer.start()

    logger("done - watchdog started")
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()
