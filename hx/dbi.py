"""
Database interface classes
"""
import base64
import contextlib
import cfg
import pdb
import sqlite3
import string
import sys
import time
import util
import warnings

try:
    import ibm_db as db2
    import ibm_db_dbi
    db2_available = True
except ImportError:
    db2_available = False

try:
    import MySQLdb as mysql
    import _mysql_exceptions as mysql_exc
    mysql_available = True
except ImportError:
    mysql_available = False


HPSS_SECTION = 'dbi-hpss'
CRWL_SECTION = 'dbi-crawler'


# -----------------------------------------------------------------------------
class DBI_abstract(object):
    """
    DBI_abstract: Each of the specific database interface classes (DBIsqlite,
    DBImysql, etc.) inherit from this one
    """
    settable_attrl = ['cfg', 'dbname', 'host', 'username', 'password',
                      'tbl_prefix', 'timeout']

    # -------------------------------------------------------------------------
    def prefix(self, tabname):
        """
        DBI_abstract: Handle prefixing a table name with the prefix for this
        database connection unless it's already there. If the table name comes
        in with '@' on the front, that's a signal to not prefix, so we just
        strip off the '@' and return the table name.
        """
        if self.tbl_prefix == '':
            return tabname

        if tabname.startswith(self.tbl_prefix):
            return tabname
        elif tabname.startswith('@'):
            return tabname[1:]
        else:
            return self.tbl_prefix + tabname

    # -------------------------------------------------------------------------
    def retry(self, exception, payload, *args, **kwargs):
        """
        Call *payload*. If it does not throw an exception, we're done --
        return. OTOH, if it does throw an exception, we call the database
        specific err_handler(). It will either throw a DBIerror up the stack
        and we're done, or it will sleep an appropriate amount of time and
        return so we can keep trying again until the timeout expires.

        The timeout is set when the DBI object is instantiated.
        """
        rval = None
        done = False
        start = time.time()
        while not done and time.time() - start < self.timeout:
            try:
                rval = payload(*args, **kwargs)
                done = True
            except exception as e:
                self.err_handler(e)
        return rval


# -----------------------------------------------------------------------------
class DBI(object):
    """
    This is the generic database interface object. The application uses this
    class so it doesn't have to know anything about talking to the database
    type actually in use.

    When a DBI object is created, it looks for an argument named "dbtype" in
    kwargs that should contain "sqlite", "mysql", or "db2".

    The DBI creates an internal object of the appropriate type and then
    forwards all method calls to it.
    """
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        DBI: Here we look for configuration information indicating which kind
        of database to use. In the cfg argument, the caller can pass us a
        CrawlConfig object. If the cfg argument is not present, we check the
        default configuration ('crawl.cfg'). Anything else in the cfg argument
        (i.e., not a CrawlConfig object) will generate an exception.

        Once we know which kind of database to use, we create an object
        specific to that database type and forward calls from the caller to
        that object.

        The database-specific class should initialize a connection to the
        database, setting autocommit mode so that we don't have to commit every
        little thing we do. If our operational mode ever becomes complicated
        enough, we may need more control over commits but autocommit will do
        for now.
        """

        # Valid arguments in kwargs are:
        #   'cfg' - a CrawlConfig object
        #   'dbtype' - 'hpss' or 'crawler'
        #   'dbname' - if dbtype is 'hpss', this must be 'cfg' or 'sub'
        #   'timeout' - max length of time to retry failing operations
        for key in kwargs:
            if key not in ['cfg', 'dbtype', 'dbname', 'timeout']:
                raise DBIerror(MSG.invalid_attr_SS % (key, self.__class__))
        if 0 < len(args):
            if not isinstance(args[0], CrawlConfig.CrawlConfig):
                raise DBIerror(MSG.unrecognized_arg_S % self.__class__)
            else:
                cfg = args[0]
        elif 'cfg' in kwargs and kwargs['cfg'] is not None:
            cfg = kwargs['cfg']
        else:
            cfg = CrawlConfig.add_config()

        dbtype = ''
        dbname = ''
        tbl_pfx = ''
        if 'dbtype' not in kwargs:
            raise DBIerror(MSG.valid_dbtype)
        elif kwargs['dbtype'] == 'hpss':
            cfg_section = HPSS_SECTION
            dbtype = cfg.get(HPSS_SECTION, 'dbtype')
            tbl_pfx = cfg.get(HPSS_SECTION, 'tbl_prefix')
            if 'dbname' not in kwargs:
                raise DBIerror("With dbtype=%s, dbname must be specified" %
                               kwargs['dbtype'])
            elif kwargs['dbname'] not in cfg.options(HPSS_SECTION):
                raise DBIerror("dbname %s not defined in the configuration" %
                               kwargs['dbname'])
            else:
                dbname = cfg.get(HPSS_SECTION, kwargs['dbname'])
        elif kwargs['dbtype'] == 'crawler':
            cfg_section = CRWL_SECTION
            if 'dbname' in kwargs:
                raise DBIerror("dbname may not be specified here")
            dbtype = cfg.get(CRWL_SECTION, 'dbtype')
            try:
                dbname = cfg.get(CRWL_SECTION, 'dbname')
            except CrawlConfig.NoOptionError as e:
                raise DBIerror(e)
            tbl_pfx = cfg.get(CRWL_SECTION, 'tbl_prefix')
        else:
            raise DBIerror(MSG.valid_dbtype)

        okw = {}
        okw['cfg'] = cfg
        okw['dbname'] = dbname
        okw['tbl_prefix'] = tbl_pfx

        if 'timeout' in kwargs:
            okw['timeout'] = kwargs['timeout']
        else:
            okw['timeout'] = cfg.get_time(cfg_section, 'timeout', 3600)

        self.closed = False
        if dbtype == 'sqlite':
            self._dbobj = DBIsqlite(*args, **okw)
        elif dbtype == 'mysql':
            self._dbobj = DBImysql(*args, **okw)
        elif dbtype == 'db2':
            self._dbobj = DBIdb2(*args, **okw)
        else:
            raise DBIerror(Msg.unknown_dbtype)

        self.dbname = self._dbobj.dbname

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        DBI: Human readable representation for the object provided by the
        database-specific class.
        """
        rv = self._dbobj.__repr__()
        if self.closed:
            rv = "[closed]" + rv
        return rv

    # -------------------------------------------------------------------------
    def alter(self, **kwargs):
        """
        DBI: Alter a table as indicated by the arguments.
        Syntax:
          db.alter(table=<tabname>, addcol=<col desc>, pos='first|after <col>')
          db.alter(table=<tabname>, dropcol=<col name>)
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.alter(**kwargs)

    # -------------------------------------------------------------------------
    def table_exists(self, **kwargs):
        """
        DBI: Return True if the table argument is not empty and the named table
        exists (even if the table itself is empty). Otherwise, return False.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.table_exists(**kwargs)

    # -------------------------------------------------------------------------
    def close(self):
        """
        DBI: Close the connection to the database. After a call to close(),
        operations are not allowed on the database.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        rv = self._dbobj.close()
        self.closed = True
        return rv

    # -------------------------------------------------------------------------
    def create(self, **kwargs):
        """
        DBI: Create the named table containing the fields listed. The fields
        list contains column specifications, for example:

            ['id int primary key', 'name text', 'category xtext', ... ]
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.create(**kwargs)

    # -------------------------------------------------------------------------
    def cursor(self, **kwargs):
        """
        DBI: Return a database cursor
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.cursor(**kwargs)

    # -------------------------------------------------------------------------
    def delete(self, **kwargs):
        """
        DBI: Delete data from the table. table is a table name (string). where
        is a where clause (string). data is a tuple of fields.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.delete(**kwargs)

    # -------------------------------------------------------------------------
    def describe(self, **kwargs):
        """
        DBI: Return a table description.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.describe(**kwargs)

    # -------------------------------------------------------------------------
    def drop(self, **kwargs):
        """
        DBI: Drop the named table.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.drop(**kwargs)

    # -------------------------------------------------------------------------
    def insert(self, **kwargs):
        """
        DBI: Insert data into the table. Fields is a list of field names. Data
        is a list of tuples.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.insert(**kwargs)

    # -------------------------------------------------------------------------
    def select(self, **kwargs):
        """
        DBI: Retrieve data from the table. Table name must be present. If
        fields is empty, all fields are selected.

        If the where argument is empty, all rows are selected and returned. If
        it contains an expression like 'id < 5', only the matching rows are
        selected. The where argument may contain something like 'name = ?', in
        which case data should be a tuple containing the matching value(s) for
        the where clause.

        If orderby is empty, the rows are returned in the order they are
        retrieved from the database. If orderby contains an field name, the
        rows are returned in that order.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.select(**kwargs)

    # -------------------------------------------------------------------------
    def update(self, **kwargs):
        """
        DBI: Update data in the table. Where indicates which records are to be
        updated. Fields is a list of field names. Data is a list of tuples.
        """
        if self.closed:
            raise DBIerror(MSG.db_closed, dbname=self._dbobj.dbname)
        return self._dbobj.update(**kwargs)


# -----------------------------------------------------------------------------
class DBIerror(Exception):
    """
    This class is used to return DBI errors to the application so it doesn't
    have to know anything about specific error types associated with the
    various database types.
    """
    def __init__(self, value, dbname=None):
        """
        DBIerror: Set the value for the exception. It should be a string.
        """
        self.value = str(value)
        self.dbname = dbname

    def __str__(self):
        """
        DBIerror: Report the exception value (should be a string).
        """
        return "%s (dbname=%s)" % (str(self.value), self.dbname)


# -----------------------------------------------------------------------------
class DBIsqlite(DBI_abstract):
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        DBIsqlite: See DBI.__init__()
        """
        for attr in kwargs:
            if attr in self.settable_attrl:
                setattr(self, attr, kwargs[attr])
            else:
                raise DBIerror(MSG.invalid_attr_SS % (attr, self.__class__))
        if not hasattr(self, 'dbname'):
            raise DBIerror(MSG.dbname_required)
        if not hasattr(self, 'tbl_prefix'):
            raise DBIerror("A table prefix is required")

        if self.tbl_prefix != '':
            self.tbl_prefix = self.tbl_prefix.rstrip('_') + '_'
        try:
            self.dbh = sqlite3.connect(self.dbname)
            # set autocommit mode
            self.dbh.isolation_level = None
            self.table_exists(table="sqlite_master")
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        DBIsqlite: See DBI.__repr__()
        """
        rv = "DBIsqlite(dbname='%s')" % self.dbname
        return rv

    # -------------------------------------------------------------------------
    def err_handler(self, err):
        """
        DBIsqlite: error handler
        """
        raise DBIerror(''.join(err.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def alter(self, table='', addcol=None, dropcol=None, pos=None):
        """
        DBIsqlite: Sqlite ignores pos if it's set and does not support dropcol.
        """
        if type(table) != str:
            raise DBIerror(MSG.alter_table_string,
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On alter(), table name must not be empty",
                           dbname=self.dbname)
        elif dropcol is not None:
            raise DBIerror("SQLite does not support dropping columns",
                           dbname=self.dbname)
        elif addcol is None:
            raise DBIerror("ALTER requires an action")
        elif addcol.strip() == "":
            raise DBIerror("On alter(), addcol must not be empty")
        elif any([x in addcol for x in ['"', "'", ';', '=']]):
            raise DBIerror("Invalid addcol argument")

        try:
            cmd = ("alter table %s add column %s" %
                   (self.prefix(table), addcol))
            c = self.dbh.cursor()
            c.execute(cmd)
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def close(self):
        """
        DBIsqlite: See DBI.close()
        """
        # Close the database connection
        if hasattr(self, "sqlite_closed") and self.sqlite_closed:
            raise DBIerror("closing a closed connection", dbname=self.dbname)
        self.sqlite_closed = True
        try:
            self.dbh.close()
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def create(self, table='', fields=[]):
        """
        DBIsqlite: See DBI.create()
        """

        if type(fields) != list:
            raise DBIerror("On create(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On create(), fields must not be empty",
                           dbname=self.dbname)
        if type(table) != str:
            raise DBIerror(MSG.create_table_string,
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On create(), table name must not be empty",
                           dbname=self.dbname)

        # Construct and run the create statement
        try:
            cmd = ("create table %s(" % self.prefix(table) +
                   ", ".join(fields) +
                   ")")
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def cursor(self):
        """
        DBIsqlite: See DBI.cursor()
        """
        try:
            rval = self.dbh.cursor()
            return rval
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def delete(self, table='', where='', data=()):
        """
        DBIsqlite: See DBI.delete()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On delete(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On delete(), table name must not be empty",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On delete(), where clause must be a string",
                           dbname=self.dbname)
        elif type(data) != tuple:
            raise DBIerror("On delete(), data must be a tuple",
                           dbname=self.dbname)
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored", dbname=self.dbname)
        elif '?' in where and data == ():
            raise DBIerror("Criteria are not fully specified",
                           dbname=self.dbname)

        # Build and run the statement
        try:
            cmd = "delete from %s" % self.prefix(table)
            if where != '':
                cmd += " where %s" % where

            c = self.dbh.cursor()
            if '?' in cmd:
                c.execute(cmd, data)
            else:
                c.execute(cmd)

            c.close()
        # Translate any sqlite3 errors to DBIerror
        except sqlite3.Error as e:
            raise DBIerror(cmd + ': ' + ''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def describe(self, table=''):
        """
        DBIsqlite: Return the description of a table.
        """
        cmd = "pragma table_info(%s)" % self.prefix(table)
        try:
            c = self.dbh.cursor()
            c.execute(cmd)
            rows = c.fetchall()
        except sqlite3.Error as e:
            self.err_handler(e)

        if [] == rows:
            raise DBIerror(MSG.no_such_table_S % self.prefix(table))

        return rows

    # -------------------------------------------------------------------------
    def drop(self, table=''):
        """
        DBIsqlite: See DBI.create()
        """
        # Handle bad arguments
        if type(table) != str:
            raise DBIerror("On drop(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On drop(), table name must not be empty",
                           dbname=self.dbname)

        # Construct and run the drop statement
        try:
            cmd = ("drop table %s" % self.prefix(table))
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def insert(self, table='', ignore=False, fields=[], data=[]):
        """
        DBIsqlite: See DBI.insert()
        """
        # Handle any bad arguments
        if type(table) != str:
            raise DBIerror("On insert(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On insert(), table name must not be empty",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On insert(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On insert(), fields list must not be empty",
                           dbname=self.dbname)
        elif type(data) != list:
            raise DBIerror("On insert(), data must be a list",
                           dbname=self.dbname)
        elif data == []:
            raise DBIerror("On insert(), data list must not be empty",
                           dbname=self.dbname)
        elif type(ignore) != bool:
            raise DBIerror(MSG.insert_ignore_bool, dbname=self.dbname)

        # Construct and run the insert statement
        try:
            cmd = ("insert %s" % ("or ignore " if ignore else "") +
                   "into %s(" % self.prefix(table) +
                   ",".join(fields) +
                   ") values (" +
                   ",".join(["?" for x in fields]) +
                   ")")
            c = self.dbh.cursor()
            c.executemany(cmd, data)
            c.close()
        # Translate sqlite specific exception into a DBIerror
        except sqlite3.Error as e:
            raise DBIerror(cmd + ": " + ''.join(e.args),
                           dbname=self.dbname)

    # -------------------------------------------------------------------------
    def select(self, table='',
               fields=[],
               where='',
               data=(),
               groupby='',
               orderby='',
               limit=None):
        """
        DBIsqlite: See DBI.select()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On select(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On select(), table name must not be empty",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On select(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("Wildcard selects are not supported." +
                           " Please supply a list of fields.",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On select(), where clause must be a string",
                           dbname=self.dbname)
        elif type(data) != tuple:
            raise DBIerror("On select(), data must be a tuple",
                           dbname=self.dbname)
        elif type(groupby) != str:
            raise DBIerror("On select(), groupby clause must be a string",
                           dbname=self.dbname)
        elif type(orderby) != str:
            raise DBIerror("On select(), orderby clause must be a string",
                           dbname=self.dbname)
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored",
                           dbname=self.dbname)
        elif limit is not None and type(limit) not in [int, float]:
            raise DBIerror("On select(), limit must be an int")

        # Build and run the select statement
        try:
            cmd = "select "
            cmd += ",".join(fields)
            cmd += " from %s" % self.prefix(table)
            if where != '':
                cmd += " where %s" % where
            if groupby != '':
                cmd += " group by %s" % groupby
            if orderby != '':
                cmd += " order by %s" % orderby
            if limit is not None:
                cmd += " limit %d" % int(limit)

            c = self.dbh.cursor()
            if '?' in cmd:
                c.execute(cmd, data)
            else:
                c.execute(cmd)
            rv = c.fetchall()
            c.close()
            return rv
        # Translate any sqlite3 errors to DBIerror
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args),
                           dbname=self.dbname)

    # -------------------------------------------------------------------------
    def table_exists(self, table=''):
        """
        DBIsqlite: See DBI.table_exists()
        """
        try:
            dbc = self.dbh.cursor()
            dbc.execute("""
                        select name from sqlite_master
                        where type='table'
                        and name=?
                        """, (self.prefix(table),))
            rows = dbc.fetchall()
            dbc.close()
            if 0 == len(rows):
                return False
            elif 1 == len(rows):
                return True
            else:
                raise DBIerror(MSG.more_than_one_ss % ('sqlite_master', table))
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def update(self, table='', where='', fields=[], data=[]):
        """
        DBIsqlite: See DBI.update()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On update(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On update(), table name must not be empty",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On update(), where clause must be a string",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On update(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On update(), fields must not be empty",
                           dbname=self.dbname)
        elif type(data) != list:
            raise DBIerror("On update(), data must be a list of tuples",
                           dbname=self.dbname)
        elif data == []:
            raise DBIerror("On update(), data must not be empty",
                           dbname=self.dbname)
        elif '"?"' in where or "'?'" in where:
            raise DBIerror("Parameter placeholders should not be quoted")

        # Build and run the update statement
        try:
            cmd = "update %s" % self.prefix(table)
            cmd += " set %s" % ",".join(["%s=?" % x for x in fields])
            if where != '':
                cmd += " where %s" % where

            c = self.dbh.cursor()
            c.executemany(cmd, data)
            c.close()
        # Translate database-specific exceptions into DBIerrors
        except sqlite3.Error as e:
            raise DBIerror(''.join(e.args),
                           dbname=self.dbname)


if mysql_available:
    # -------------------------------------------------------------------------
    class DBImysql(DBI_abstract):
        # ---------------------------------------------------------------------
        def __init__(self, *args, **kwargs):
            """
            DBImysql: See DBI.__init__()
            """
            for attr in kwargs:
                if attr in self.settable_attrl:
                    setattr(self, attr, kwargs[attr])
                else:
                    raise DBIerror(MSG.invalid_attr_SS % (attr,
                                                          self.__class__))
            if not hasattr(self, 'dbname'):
                raise DBIerror(MSG.dbname_required)
            if not hasattr(self, 'tbl_prefix'):
                raise DBIerror("A table prefix is required")

            if self.tbl_prefix != '':
                self.tbl_prefix = self.tbl_prefix.rstrip('_') + '_'
            cfg = kwargs['cfg']
            if not hasattr(self, 'timeout'):
                self.timeout = cfg.get_time(CRWL_SECTION, 'timeout', 3600)
            host = cfg.get(CRWL_SECTION, 'hostname')
            username = cfg.get(CRWL_SECTION, 'username')
            password = base64.b64decode(cfg.get(CRWL_SECTION, 'password'))

            self.dbh = self.retry(mysql_exc.Error,
                                  mysql.connect,
                                  host=host,
                                  user=username,
                                  passwd=password,
                                  db=self.dbname)
            self.dbh.autocommit(True)

        # ---------------------------------------------------------------------
        def __repr__(self):
            """
            DBImysql: See DBI.__repr__()
            """
            rv = "DBImysql(dbname='%s')" % self.dbname
            return rv

        # ---------------------------------------------------------------------
        def err_handler(self, err):
            """
            DBImysql: Error handler. If this returns, it should be called in a
            loop to retry operations that are having transient failures.
            """
            if isinstance(err, mysql_exc.ProgrammingError):
                raise DBIerror(str(err), dbname=self.dbname)
            elif 1 < len(err.args) and err.args[0] in [1047, 2003]:
                print("RETRY")
                if not hasattr(self, 'sleeptime'):
                    self.sleeptime = 0.1
                time.sleep(self.sleeptime)
                self.sleeptime = min(2*self.sleeptime, 10.0)
            else:
                raise DBIerror("%d: %s" % err.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def alter(self, table='', addcol=None, dropcol=None, pos=None):
            """
            DBImysql: Alter the table as indicated.
            """
            cmd = ''
            if type(table) != str:
                raise DBIerror("On alter(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On alter(), table name must not be empty",
                               dbname=self.dbname)
            elif addcol is not None:
                if addcol.strip() == '':
                    raise DBIerror("On alter(), addcol must not be empty",
                                   dbname=self.dbname)
                if any([x in addcol for x in ['"', "'", ';', '=']]):
                    raise DBIerror("Invalid addcol argument")
                if pos:
                    cmd = ("alter table %s add column %s %s" %
                           (self.prefix(table), addcol, pos))
                else:
                    cmd = ("alter table %s add column %s" %
                           (self.prefix(table), addcol))
            elif dropcol is not None:
                if dropcol.strip() == '':
                    raise DBIerror("On alter, dropcol must not be empty",
                                   dbname=self.dbname)
                if any([x in dropcol for x in ['"', "'", ';', '=']]):
                    raise DBIerror("Invalid dropcol argument")
                cmd = ("alter table %s drop column %s" %
                       (self.prefix(table), dropcol))
            if cmd == '':
                raise DBIerror("ALTER requires an action")

            try:
                c = self.dbh.cursor()
                c.execute(cmd)
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def close(self):
            """
            DBImysql: See DBI.close()
            """
            # Close the database connection
            try:
                self.dbh.close()
            # Convert any mysql error into a DBIerror
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def create(self, table='', fields=[]):
            """
            DBImysql: See DBI.create()
            """
            # Handle bad arguments
            if type(fields) != list:
                raise DBIerror("On create(), fields must be a list",
                               dbname=self.dbname)
            elif fields == []:
                raise DBIerror("On create(), fields must not be empty",
                               dbname=self.dbname)
            if type(table) != str:
                raise DBIerror("On create(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On create(), table name must not be empty",
                               dbname=self.dbname)

            # Construct and run the create statement
            mysql_f = [x.replace('autoincrement', 'auto_increment')
                       for x in fields]
            try:
                cmd = ("create table %s(" % self.prefix(table) +
                       ", ".join(mysql_f) +
                       ") engine = innodb")
                c = self.dbh.cursor()
                c.execute(cmd)

            # Convert any db specific error into a DBIerror
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def cursor(self):
            """
            DBImysql: get a cursor
            """
            try:
                rval = self.dbh.cursor()
                return rval
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def delete(self, table='', where='', data=()):
            """
            DBImysql: delete records
            """
            # Handle invalid arguments
            if type(table) != str:
                raise DBIerror("On delete(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On delete(), table name must not be empty",
                               dbname=self.dbname)
            elif type(where) != str:
                raise DBIerror("On delete(), where clause must be a string",
                               dbname=self.dbname)
            elif type(data) != tuple:
                raise DBIerror("On delete(), data must be a tuple",
                               dbname=self.dbname)
            elif '?' not in where and data != ():
                raise DBIerror("Data would be ignored", dbname=self.dbname)
            elif '?' in where and data == ():
                raise DBIerror("Criteria are not fully specified",
                               dbname=self.dbname)

            # Build and run the statement
            try:
                cmd = "delete from %s" % self.prefix(table)
                if where != '':
                    cmd += " where %s" % where.replace('?', '%s')

                c = self.dbh.cursor()
                if '%s' in cmd:
                    c.execute(cmd, data)
                else:
                    c.execute(cmd)

                c.close()
            # Translate any db specific errors to DBIerror
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def describe(self, table=''):
            """
            DBImysql: Return a table description
            """
            try:
                cmd = """select column_name, ordinal_position, data_type
                             from information_schema.columns
                             where table_name = %s"""
                c = self.dbh.cursor()
                c.execute(cmd, (self.prefix(table),))
                r = c.fetchall()
            except mysql_exc.Error as e:
                self.err_handler(e)

            if 0 == len(r):
                raise DBIerror(MSG.no_such_table_S % self.prefix(table))

            return r

        # ---------------------------------------------------------------------
        def drop(self, table=''):
            """
            DBImysql: Drop a mysql table
            """
            # Handle bad arguments
            if type(table) != str:
                raise DBIerror("On drop(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On drop(), table name must not be empty",
                               dbname=self.dbname)

            # Construct and run the drop statement
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore",
                                            "Unknown table '.*'")
                    cmd = ("drop table %s" % self.prefix(table))
                    c = self.dbh.cursor()
                    c.execute(cmd)

            # Convert any db specific error into a DBIerror
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def insert(self, table='', ignore=False, fields=[], data=[]):
            """
            DBImysql: Insert into a mysql database
            """
            # Handle any bad arguments
            if type(table) != str:
                raise DBIerror("On insert(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On insert(), table name must not be empty",
                               dbname=self.dbname)
            elif type(fields) != list:
                raise DBIerror("On insert(), fields must be a list",
                               dbname=self.dbname)
            elif fields == []:
                raise DBIerror("On insert(), fields list must not be empty",
                               dbname=self.dbname)
            elif type(data) != list:
                raise DBIerror("On insert(), data must be a list",
                               dbname=self.dbname)
            elif data == []:
                raise DBIerror("On insert(), data list must not be empty",
                               dbname=self.dbname)
            elif type(ignore) != bool:
                raise DBIerror(MSG.insert_ignore_bool, dbname=self.dbname)

            # Construct and run the insert statement
            try:
                cmd = ("insert %s" % ("ignore " if ignore else "") +
                       "into %s(" % self.prefix(table) +
                       ",".join(fields) +
                       ") values (" +
                       ",".join(["%s" for x in fields]) +
                       ")")
                c = self.dbh.cursor()
                if ignore:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore",
                                                "Duplicate entry .*")
                        c.executemany(cmd, data)
                else:
                    c.executemany(cmd, data)
                c.close()
            # Translate sqlite specific exception into a DBIerror
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def select(self,
                   table='',
                   fields=[],
                   where='',
                   data=(),
                   groupby='',
                   orderby='',
                   limit=None):
            """
            DBImysql: Select from a mysql database.
            """
            # Handle invalid arguments
            if type(table) != str:
                raise DBIerror("On select(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On select(), table name must not be empty",
                               dbname=self.dbname)
            elif type(fields) != list:
                raise DBIerror("On select(), fields must be a list",
                               dbname=self.dbname)
            elif fields == []:
                raise DBIerror("Wildcard selects are not supported." +
                               " Please supply a list of fields.",
                               dbname=self.dbname)
            elif type(where) != str:
                raise DBIerror("On select(), where clause must be a string",
                               dbname=self.dbname)
            elif type(data) != tuple:
                raise DBIerror("On select(), data must be a tuple",
                               dbname=self.dbname)
            elif type(groupby) != str:
                raise DBIerror("On select(), groupby clause must be a string",
                               dbname=self.dbname)
            elif type(orderby) != str:
                raise DBIerror("On select(), orderby clause must be a string",
                               dbname=self.dbname)
            elif '?' not in where and data != ():
                raise DBIerror("Data would be ignored",
                               dbname=self.dbname)
            elif limit is not None and type(limit) not in [int, float]:
                raise DBIerror("On select(), limit must be an int")

            # Build and run the select statement
            cmd = "select "
            cmd += ",".join(fields)
            cmd += " from %s" % self.prefix(table)
            if where != '':
                cmd += " where %s" % where.replace('?', '%s')
            if groupby != '':
                cmd += " group by %s" % groupby
            if orderby != '':
                cmd += " order by %s" % orderby
            if limit is not None:
                cmd += " limit 0, %d" % int(limit)

            rv = self.retry(mysql_exc.Error,
                            self.do_select,
                            cmd,
                            data)
            return rv

        # ---------------------------------------------------------------------
        def do_select(self, cmd, data=None):
            """
            Routine select has set everything up. These are the calls that
            might throw an exception that we want to run under retry(), so they
            need to be isolated in this routine.
            """
            c = self.dbh.cursor()
            # if data:
            if '%s' in cmd:
                c.execute(cmd, data)
            else:
                c.execute(cmd)
            rval = c.fetchall()
            c.close()
            return rval

        # ---------------------------------------------------------------------
        def table_exists(self, table=''):
            """
            DBImysql: Check whether a table exists in a mysql database
            """
            try:
                dbc = self.dbh.cursor()
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore",
                                            "Can't read dir of .*")
                    dbc.execute("""
                                select table_name
                                from information_schema.tables
                                where table_name=%s
                                """, (self.prefix(table),))
                rows = dbc.fetchall()
                dbc.close()
                if 0 == len(rows):
                    return False
                elif 1 == len(rows):
                    return True
                else:
                    raise DBIerror(MSG.more_than_one_ss %
                                   ('information_schema.tables', table))
            except mysql_exc.Error as e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def update(self, table='', where='', fields=[], data=[]):
            """
            DBImysql: See DBI.update()
            """
            # Handle invalid arguments
            if type(table) != str:
                raise DBIerror("On update(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On update(), table name must not be empty",
                               dbname=self.dbname)
            elif type(where) != str:
                raise DBIerror("On update(), where clause must be a string",
                               dbname=self.dbname)
            elif type(fields) != list:
                raise DBIerror("On update(), fields must be a list",
                               dbname=self.dbname)
            elif fields == []:
                raise DBIerror("On update(), fields must not be empty",
                               dbname=self.dbname)
            elif type(data) != list:
                raise DBIerror("On update(), data must be a list of tuples",
                               dbname=self.dbname)
            elif data == []:
                raise DBIerror("On update(), data must not be empty",
                               dbname=self.dbname)
            elif '"?"' in where or "'?'" in where:
                raise DBIerror("Parameter placeholders should not be quoted")

            # Build and run the update statement
            try:
                cmd = "update %s" % self.prefix(table)
                cmd += " set %s" % ",".join(["%s=" % x + "%s" for x in fields])
                if where != '':
                    cmd += " where %s" % where.replace('?', '%s')

                c = self.dbh.cursor()
                c.executemany(cmd, data)
                c.close()
            # Translate database-specific exceptions into DBIerrors
            except mysql_exc.Error as e:
                self.err_handler(e)


if db2_available:
    # -----------------------------------------------------------------------------
    class DBIdb2(DBI_abstract):
        # -------------------------------------------------------------------------
        def __init__(self, *args, **kwargs):
            """
            DBIdb2: See DBI.__init__()
            """
            for attr in kwargs:
                if attr in self.settable_attrl:
                    setattr(self, attr, kwargs[attr])
                else:
                    raise DBIerror(MSG.invalid_attr_SS % (attr,
                                                          self.__class__))
            if not hasattr(self, 'dbname'):
                raise DBIerror(MSG.dbname_required)
            if not hasattr(self, 'tbl_prefix'):
                self.tbl_prefix = 'hpss'

            if self.tbl_prefix != '':
                self.tbl_prefix = self.tbl_prefix.rstrip('.') + '.'
            try:
                cfg = kwargs['cfg']
            except:
                cfg = CrawlConfig.get_config()

            if not hasattr(self, 'timeout'):
                self.timeout = cfg.get_time(HPSS_SECTION, 'timeout', 3600)
            util.env_update(cfg)
            host = cfg.get(HPSS_SECTION, 'hostname')
            port = cfg.get(HPSS_SECTION, 'port')
            try:
                username = cfg.get(HPSS_SECTION, 'username')
            except:
                username = ''
            try:
                password = base64.b64decode(cfg.get(HPSS_SECTION, 'password'))
            except:
                password = ''

            cxnstr = ("database=%s;" % self.dbname +
                      "hostname=%s;" % host +
                      "port=%s;" % port +
                      "uid=%s;" % username +
                      "pwd=%s;" % password)
            self.dbh = self.retry(Exception,
                                  db2.connect,
                                  cxnstr,
                                  "",
                                  "")

        # ---------------------------------------------------------------------
        def __repr__(self):
            """
            DBIdb2: See DBI.__repr__()
            """
            rv = "DBIdb2(dbname='%s')" % self.dbname
            return rv

        # ---------------------------------------------------------------------
        def err_handler(self, err=None, message=''):
            """
            DBIdb2: error handler can accept a string or an exception object
            """
            if err is None:
                raise DBIerror(message, dbname=self.dbname)
            elif isinstance(err, ibm_db_dbi.Error):
                raise DBIerror("%d: %s" % err.args, dbname=self.dbname)
            elif 'A communication error has been detected' in str(err):
                if not hasattr(self, 'sleeptime'):
                    self.sleeptime = 0.1
                CrawlConfig.log('Riding out DB2 outage -- sleeping %f seconds'
                                % self.sleeptime)
                time.sleep(self.sleeptime)
                self.sleeptime = min(2*self.sleeptime, 60.0)
            else:
                raise DBIerror(str(err), dbname=self.dbname)

        # ---------------------------------------------------------------------
        def __recognized_exception__(self, exc):
            """
            DBIdb2: Return True if exc is recognized as a DB2 error. Otherwise
            False.
            """
            try:
                q = self.db2_exc_list
            except AttributeError:
                self.db2_exc_list = ["params bound not matching",
                                     "SQLSTATE=",
                                     "[IBM][CLI Driver][DB2"]
            rval = False
            for x in self.db2_exc_list:
                if x in str(exc):
                    rval = True
                    break
            return rval

        # ---------------------------------------------------------------------
        def alter(self, table='', addcol=None, dropcol=None, pos=None):
            """
            DBIdb2: See DBI.alter()
            """
            raise DBIerror(MSG.db2_unsupported_S % "ALTER")

        # ---------------------------------------------------------------------
        def close(self):
            """
            DBIdb2: See DBI.close()
            """
            # Close the database connection
            try:
                db2.close(self.dbh)
            # Convert any db2 error into a DBIerror
            except Exception as e:
                self.err_handler(err=e)

        # ---------------------------------------------------------------------
        def create(self, table='', fields=[]):
            """
            DBIdb2: See DBI.create()
            """
            raise DBIerror(MSG.db2_unsupported_S % "CREATE")

        # ---------------------------------------------------------------------
        def cursor(self):
            """
            DBIdb2: See DBI.cursor()
            """
            try:
                rval = self.dbh.cursor()
                return rval
            except mysql_exc.Error as e:
                raise DBIerror("%d: %s" % e.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def delete(self, **kwargs):
            """
            DBIdb2: See DBI.delete()
            """
            raise DBIerror(MSG.db2_unsupported_S % "DELETE")

        # ---------------------------------------------------------------------
        def describe(self, **kwargs):
            """
            DBIdb2: Return a table description
            """
            raise DBIerror(MSG.db2_unsupported_S % "DESCRIBE")

        # ---------------------------------------------------------------------
        def drop(self, table=''):
            """
            DBIdb2:
            """
            raise DBIerror(MSG.db2_unsupported_S % "DROP")

        # ---------------------------------------------------------------------
        def insert(self, table='', fields=[], data=[]):
            """
            DBIdb2: Insert not supported for DB2
            """
            raise DBIerror(MSG.db2_unsupported_S % "INSERT")

        # ---------------------------------------------------------------------
        def select(self,
                   table='',
                   fields=[],
                   where='',
                   data=(),
                   groupby='',
                   orderby='',
                   limit=None):
            """
            DBIdb2: Select from a DB2 database.
            """
            # Handle invalid arguments
            if type(table) != str and type(table) != list:
                raise DBIerror("On select(), table name must be " +
                               "a string or a list",
                               dbname=self.dbname)
            elif table == '' or table == []:
                raise DBIerror("On select(), table name must not be empty",
                               dbname=self.dbname)
            elif type(fields) != list:
                raise DBIerror("On select(), fields must be a list",
                               dbname=self.dbname)
            elif fields == []:
                raise DBIerror("Wildcard selects are not supported." +
                               " Please supply a list of fields.",
                               dbname=self.dbname)
            elif type(where) != str:
                raise DBIerror("On select(), where clause must be a string",
                               dbname=self.dbname)
            elif type(data) != tuple:
                raise DBIerror("On select(), data must be a tuple",
                               dbname=self.dbname)
            elif type(groupby) != str:
                raise DBIerror("On select(), groupby clause must be a string",
                               dbname=self.dbname)
            elif type(orderby) != str:
                raise DBIerror("On select(), orderby clause must be a string",
                               dbname=self.dbname)
            elif '?' not in where and data != ():
                raise DBIerror("Data would be ignored",
                               dbname=self.dbname)
            elif limit is not None and type(limit) not in [int, float]:
                raise DBIerror("On select(), limit must be an int")

            # Build and run the select statement
            try:
                cmd = "select "
                cmd += ",".join(fields)

                if type(table) == str:
                    cmd += " from %s" % self.prefix(table)
                elif type(table) == list:
                    cmd += " from %s" % ",".join([self.prefix(x)
                                                  for x in table])

                if where != '':
                    cmd += " where %s" % where
                if groupby != '':
                    cmd += " group by %s" % groupby
                if orderby != '':
                    cmd += " order by %s" % orderby
                if limit is not None:
                    cmd += " fetch first %d rows only" % int(limit)

                rval = []
                stmt = db2.prepare(self.dbh, cmd)
                args = [stmt]
                if '?' in cmd:
                    args.append(data)
                r = db2.execute(*args)
                x = db2.fetch_assoc(stmt)
                while (x):
                    rval.append(x)
                    x = db2.fetch_assoc(stmt)

                return rval

            # Translate any db2 errors to DBIerror
            except ibm_db_dbi.Error as e:
                errmsg = str(e) + "\nSQL: '" + cmd + "'"
                raise DBIerror(errmsg, dbname=self.dbname)
            except Exception as e:
                if self.__recognized_exception__(e):
                    errmsg = str(e) + "\nSQL: '" + cmd + "'"
                    raise DBIerror(errmsg, dbname=self.dbname)
                else:
                    raise

        # ---------------------------------------------------------------------
        def table_exists(self, table=''):
            """
            DBIdb2: Check whether a table exists in a db2 database
            """
            try:
                rows = self.select(table="@syscat.tables",
                                   fields=['tabname'],
                                   where="tabschema = 'HPSS' and " +
                                   "tabname = '%s'" % table.upper())
                if 0 == len(rows):
                    return False
                elif 1 == len(rows):
                    return True
                else:
                    raise DBIerror(MSG.more_than_one_ss %
                                   ('@syscat.tables', table))
            except mysql_exc.Error as e:
                raise DBIerror("%d: %s" % e.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def update(self, table='', where='', fields=[], data=[]):
            """
            DBIdb2: See DBI.update()
            """
            raise DBIerror(MSG.db2_unsupported_S % "UPDATE")

        # ---------------------------------------------------------------------
        @classmethod
        def hexstr(cls, bfid):
            """
            DBIdb2: Convert a raw bitfile id into a hexadecimal string as
            presented by DB2.
            """
            rval = "x'" + DBIdb2.hexstr_uq(bfid) + "'"
            return rval

        @classmethod
        # ---------------------------------------------------------------------
        def hexstr_uq(cls, bfid):
            """
            DBIdb2: Convert a raw bitfile id into an unquoted hexadecimal
            string as presented by DB2.
            """
            rval = "".join(["%02x" % ord(c) for c in list(bfid)])
            return rval.upper()

        @classmethod
        # ---------------------------------------------------------------------
        def hexval(cls, bfid_str):
            """
            DBIdb2: Convert a quoted or unquoted hexadecimal string as
            presented by DB2 into a hex value.
            """
            bfid_low = bfid_str.lower()
            if bfid_low.startswith("x'"):
                rval = bfid_low.strip("x'").decode("hex")
            elif all(c in string.hexdigits for c in bfid_str):
                rval = bfid_str.decode("hex")
            elif (bfid_low.startswith("x") and
                  all(c in string.hexdigits for c in bfid_low[1:])):
                rval = bfid_low[1:].decode("hex")
            else:
                rval = bfid_str

            return rval


# -----------------------------------------------------------------------------
@contextlib.contextmanager
def db_context(**kw):
    """
    Open a database connection, let the caller do something with it, then close
    it
    """
    rval = DBI(**kw)
    yield rval
    rval.close()
