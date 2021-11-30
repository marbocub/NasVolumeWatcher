#
# Copyright (c) 2021 @marbocub <marbocub@gmail.com>
# Released under the MIT license
#

import os, sys, datetime, time
from abc import ABCMeta, abstractmethod
from typing import Any, Sequence, Callable, List, Dict, Tuple
import psycopg

#----------------------------------------------------------------------
# File/Dir structure
#----------------------------------------------------------------------
class FileBase:
    volume: str
    path: str
    parent: str
    __size: int
    __ctime: datetime
    __mtime: datetime
    __atime: datetime

    def __init__(self, volume: str = None, path: str = None, parent: str = None, stat: os.stat_result = None) -> None:
        self.volume = volume
        self.path = path
        self.parent = parent
        self.stat = stat

    def __eq__(self, other):
        if not isinstance(other, FileBase):
            return False
        return (self.volume == other.volume) and (self.path == other.path)

    def __hash__(self):
        return hash(os.path.join(self.volume, self.path))

    @property
    def stat(self) -> None:
        raise NotImplementedError()
    @stat.setter
    def stat(self, stat: os.stat_result) -> None:
        if stat is None:
            self.__size = None
            self.__ctime = None
            self.__mtime = None
            self.__atime = None
        else:
            self.__size = stat.st_size
            self.__ctime = datetime.datetime.fromtimestamp(stat.st_ctime).replace(microsecond=0)
            self.__mtime = datetime.datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0)
            self.__atime = datetime.datetime.fromtimestamp(stat.st_atime).replace(microsecond=0)

    @property
    def size(self) -> int:
        return int(self.__size)
    @size.setter
    def size(self, size: int) -> None:
        self.__size = size

    @property
    def ctime(self) -> str:
        return str(self.__ctime)
    @ctime.setter
    def ctime(self, ctime: datetime.datetime) -> None:
        self.__ctime = ctime

    @property
    def mtime(self) -> str:
        return str(self.__mtime)
    @mtime.setter
    def mtime(self, mtime: datetime.datetime) -> None:
        self.__mtime = mtime

    @property
    def atime(self) -> str:
        return str(self.__atime)
    @atime.setter
    def atime(self, atime: datetime.datetime) -> None:
        self.__atime = atime

class File(FileBase):
    sha256: str
    rehash: bool

    def __init__(self, *args, sha256: str = None, rehash: bool = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sha256 = sha256
        self.rehash = rehash

class Dir(FileBase):
    count: int

    def __init__(self, *args, count: int = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.count = count

#----------------------------------------------------------------------
# Database Interface
#----------------------------------------------------------------------
class DatabaseInterface(metaclass=ABCMeta):
    @property
    @abstractmethod
    def files(self) -> List[File]:
        pass

    @files.setter
    @abstractmethod
    def files(self, files: Tuple[File]) -> None:
        pass

    @property
    @abstractmethod
    def dirs(self) -> List[Dir]:
        pass

    @dirs.setter
    @abstractmethod
    def dirs(self, dirs: Tuple[Dir]) -> None:
        pass

    @property
    @abstractmethod
    def created(self) -> List[FileBase]:
        pass

    @created.setter
    @abstractmethod
    def created(self, files: Tuple[FileBase]) -> None:
        pass

    @property
    @abstractmethod
    def updated(self) -> List[FileBase]:
        pass

    @updated.setter
    @abstractmethod
    def updated(self, files: Tuple[FileBase]) -> None:
        pass

    @property
    @abstractmethod
    def deleted(self) -> List[FileBase]:
        pass

    @deleted.setter
    @abstractmethod
    def deleted(self, files: Tuple[FileBase]) -> None:
        pass

    @property
    @abstractmethod
    def modified(self) -> List[FileBase]:
        pass

    @modified.setter
    @abstractmethod
    def modified(self, files: Tuple[FileBase]) -> None:
        pass

    @property
    @abstractmethod
    def moved(self) -> List[FileBase]:
        pass

    @moved.setter
    @abstractmethod
    def moved(self, files: Dict[FileBase,FileBase]) -> None:
        pass

#----------------------------------------------------------------------
# Database implement: PostgreSQL I/O
#----------------------------------------------------------------------
class FileRowFactory:
    def __init__(self, cursor: psycopg.Cursor) -> None:
        self.fields = [c.name for c in cursor.description]

    def __call__(self, values: Sequence[Any]) -> FileBase:
        row = dict(zip(self.fields, values))
        if 'sha256' in self.fields:
            file = File()
        else:
            file = Dir()
        for key, value in row.items():
            setattr(file, key, value)
        return file

class DatabasePostgreSQL(DatabaseInterface):
    connection: psycopg.Connection = None
    logger: Callable = None
    limit: int = 100

    def __init__(self, logger: Callable=None, limit: int=100) -> None:
        self.logger = logger or (lambda *x, **y: None)
        self.limit = limit
        self.connection = psycopg.connect(
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT'),
            dbname=os.environ.get('DB_DATABASE'),
            user=os.environ.get('DB_USERNAME'),
            password=os.environ.get('DB_PASSWORD')
        )
        self._create_tables()

    def __del__(self) -> None:
        if self.connection is not None:
            self.connection.close()

    @property
    def files(self) -> List[File]:
        raise AttributeError

    @files.setter
    def files(self, files: Tuple[File]) -> None:
        self.upsert_files_replace(files)

    @property
    def dirs(self) -> List[Dir]:
        raise AttributeError

    @dirs.setter
    def dirs(self, dirs: Tuple[Dir]) -> None:
        self.upsert_files_replace(dirs)

    @property
    def created(self) -> List[FileBase]:
        return self.select_files(param={'sha256': None})

    @created.setter
    def created(self, files: Tuple[FileBase]) -> None:
        self.upsert_files(files)

    @property
    def updated(self) -> List[FileBase]:
        raise AttributeError

    @updated.setter
    def updated(self, files: Tuple[FileBase]) -> None:
        if len(files) > 0:
            self.upsert_files(files)
        else:
            self.update_counter()

    @property
    def deleted(self) -> List[FileBase]:
        raise AttributeError

    @deleted.setter
    def deleted(self, files: Tuple[FileBase]) -> None:
        self.delete_files(files)

    @property
    def modified(self) -> List[FileBase]:
        return self.select_files(modified = True, limit=100)

    @modified.setter
    def modified(self, files: Tuple[FileBase]) -> None:
        self.set_rehash_files(files)

    @property
    def moved(self) -> List[FileBase]:
        raise AttributeError

    @moved.setter
    def moved(self, files: Dict[FileBase,FileBase]) -> None:
        self.rename_files(files)

    def _table(self, file: FileBase) -> str:
        if isinstance(file, File):
            return 'files'
        elif isinstance(file, Dir):
            return 'dirs'
        else:
            raise NotImplementedError()

    def upsert_files_replace(self, files: Tuple[FileBase]) -> None:
        if len(files) == 0:
            return

        table = self._table(files[0])
        self.logger("  updateing table: {} ({})".format(table, len(files)))
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    'create temp table if not exists files_exist ('
                        '"volume" varchar(16),'
                        '"path" varchar(512),'
                        '"parent" varchar(512) NULL,'
                        '"size" bigint NULL,'
                        '"ctime" timestamp NULL,'
                        '"mtime" timestamp NULL,'
                        '"atime" timestamp NULL'
                    ');'
                )
                cursor.execute(sql)
                cursor.execute('delete from files_exist')

                start = time.time()
                with cursor.copy('copy files_exist from stdin') as files_exist:
                    for file in files:
                        files_exist.write_row((
                            file.volume,
                            file.path,
                            file.parent,
                            file.size,
                            file.ctime,
                            file.mtime,
                            file.atime,
                        ))
                self.logger("    copy: {} sec".format(time.time()-start))

                start = time.time()
                if table == 'files':
                    update = 'update set rehash=TRUE where {}.size!=excluded.size or {}.mtime!=excluded.mtime'.format(table, table)
                else:
                    update = 'nothing'
                sql = (
                    'insert into {} (volume, path, parent, size, ctime, mtime, atime) '
                    'select volume, path, parent, size, ctime, mtime, atime from files_exist '
                    'on conflict (volume,path) do {};'
                    .format(table, update)
                )
                cursor.execute(sql)
                self.logger("    upsert: {} sec".format(time.time()-start))

                start = time.time()
                sql = (
                    'delete from {} as t '
                    'using {} as f '
                    'left join files_exist as e on f.volume=e.volume and f.path=e.path '
                    'where t.volume=f.volume and t.path=f.path '
                    'and e.path is null;'
                    .format(table, table)
                )
                cursor.execute(sql)
                self.logger("    delete: {} sec".format(time.time()-start))

                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def upsert_files(self, files: Tuple[FileBase]) -> None:
        try:
            with self.connection.cursor() as cursor:
                for file in files:
                    table = self._table(file)
                    if isinstance(file, File):
                        params = {
                            'sha256': file.sha256,
                            'rehash': file.rehash,
                        }
                    else:
                        params = {}
                    sql = (
                        "insert into {} (volume, path, parent, size, ctime, mtime, atime) "
                        "values (%s, %s, %s, %s, %s, %s, %s) "
                        "on conflict(volume, path) do update set "
                            "parent=excluded.parent,"
                            "size=excluded.size,"
                            "ctime=excluded.ctime,"
                            "mtime=excluded.mtime,"
                            "atime=excluded.atime"
                            + "".join(map(lambda x: ","+str(x)+"=%s", (params or {}).keys()))
                    ).format(table)
                    values = tuple(
                        [
                            file.volume,
                            file.path,
                            file.parent,
                            file.size,
                            file.ctime,
                            file.mtime,
                            file.atime
                        ]
                        + list((params or {}).values())
                    )
                    cursor.execute(sql, values)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def select_files(self, table: str = 'files', params: Dict[str, Any] = None, modified: bool = False, limit: int = 0) -> List[FileBase]:
        results: List[FileBase] = []
        try:
            with self.connection.cursor(row_factory=FileRowFactory) as cursor:
                condition1 = list(map(lambda x: str(x[0])+"=%s" if x[1] is not None else x[0]+" is NULL", (params or {}).items()))
                condition2 = (['(sha256 is null or rehash = TRUE)'] if modified else [])
                sql = (
                    'select * from {} '
                    'where ' + " and ".join(condition1 + condition2) + ' '
                    'order by size asc'
                    + (" limit {0}".format(limit) if limit>0 else "")
                ).format(table)
                values = list(filter(lambda x: x is not None, (params or {}).values()))
                cursor.execute(sql, values)
                results = cursor.fetchall()
        except Exception as e:
            print("exception({}): {}".format(sys._getframe().f_code.co_name, e))
        return results

    def delete_files(self, files: Tuple[FileBase]) -> None:
        try:
            with self.connection.cursor() as cursor:
                for file in files:
                    sql = 'delete from {} where volume=%s and path=%s'.format(self._table(file))
                    values = (file.volume, file.path)
                    cursor.execute(sql, values)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def set_rehash_files(self, files: Tuple[FileBase]) -> None:
        try:
            with self.connection.cursor() as cursor:
                for file in files:
                    if isinstance(file, File):
                        sql = 'update {} set rehash=TRUE where volume=%s and path=%s'.format(self._table(file))
                        values = (file.volume, file.path)
                        cursor.execute(sql, values)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def rename_files(self, files: Dict[FileBase,FileBase]) -> None:
        try:
            with self.connection.cursor() as cursor:
                for src,dst in files.items():
                    sql = 'update {} set volume=%s, path=%s, parent=%s where volume=%s and path=%s'.format(self._table(src))
                    values = (dst.volume, dst.path, dst.parent, src.volume, src.path)
                    cursor.execute(sql, values)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def update_counter(self) -> None:
        try:
            with self.connection.cursor() as cursor:
                sql = (
                    "with "
                    "hashlist (sha256) as ("
                        "select sha256 from files group by sha256 having count(sha256)>1"
                    "),"
                    "counts (volume, path, count) as ("
                        "select d.volume, d.path, count(m) "
                        "from dirs d "
                        "left join files f on d.volume=f.volume and d.path=f.parent and f.size>0 "
                        "left join hashlist m on f.sha256=m.sha256 "
                        "group by d.volume, d.path"
                    ")"
                    "update dirs set count=c.count "
                    "from counts c "
                    "where dirs.volume=c.volume and dirs.path=c.path "
                    "and (dirs.count<>c.count or dirs.count is null)"
                )
                cursor.execute(sql)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print("rollbacked({}): {}".format(sys._getframe().f_code.co_name, e))

    def _create_tables(self) -> None:
        with self.connection.cursor() as cursor:
            sql = (
                'CREATE TABLE if not exists "public"."dirs" ('
                    '"volume" character varying(16) NOT NULL,'
                    '"path" character varying(512) NOT NULL,'
                    '"parent" character varying(512),'
                    '"size" bigint,'
                    '"ctime" timestamp,'
                    '"mtime" timestamp,'
                    '"atime" timestamp,'
                    '"count" integer,'
                    'CONSTRAINT "dirs_pkey" PRIMARY KEY ("volume", "path")'
                ') WITH (oids = false);'
            )
            cursor.execute(sql)

            sql = (
                'CREATE INDEX if not exists "dirs_parent" ON "public"."dirs" USING btree ("parent");'
            )
            cursor.execute(sql)

            sql = (
                'CREATE TABLE if not exists "public"."files" ('
                    '"volume" character varying(16) NOT NULL,'
                    '"path" character varying(512) NOT NULL,'
                    '"parent" character varying(512),'
                    '"size" bigint,'
                    '"ctime" timestamp,'
                    '"mtime" timestamp,'
                    '"atime" timestamp,'
                    '"sha256" character varying(64),'
                    '"rehash" boolean,'
                    'CONSTRAINT "files_pkey" PRIMARY KEY ("volume", "path")'
                ') WITH (oids = false);'
            )
            cursor.execute(sql)
            sql = (
                'CREATE INDEX if not exists "files_parent" ON "public"."files" USING btree ("parent");'
            )
            cursor.execute(sql)
            sql = (
                'CREATE INDEX if not exists "files_sha256" ON "public"."files" USING btree ("sha256");'
            )
            cursor.execute(sql)

            sql = (
                'CREATE TABLE if not exists "public"."hashes" ('
                    '"sha256" character varying(64) NOT NULL,'
                    '"size" bigint,'
                    '"count" integer,'
                    'CONSTRAINT "hashes_pkey" PRIMARY KEY ("sha256")'
                ') WITH (oids = false);'
            )
            cursor.execute(sql)

            sql = (
                'CREATE TABLE if not exists "public"."master_dirs" ('
                    '"volume" character varying(16) NOT NULL,'
                    '"path" character varying(512) NOT NULL,'
                    'CONSTRAINT "master_dirs_pkey" PRIMARY KEY ("volume", "path")'
                ') WITH (oids = false);'
            )
            cursor.execute(sql)
        self.connection.commit()
