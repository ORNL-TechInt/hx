"""
Database interface classes

We have interface classes for mysql, sqlite, and db2.

The db2 interface only supports read operations, nothing that will change the
database. Also the db2 interface doesn't use table prefixes.

NOTE: Some of these tests depend on having access to a default config file with
valid entries in the [dbi-crawler] and [dbi-hpss] sections. This means that the
hostname, port, username, and password fields of the [dbi-hpss] section must
point to a functional DB2 server. The [dbi-crawler] section must point either
to an sqlite database or a functional mysql server.
"""
import base64
import hx.cfg
import hx.dbi
import hx.testhelp
import hx.util
import os
import pdb
import pytest
import sqlite3
import socket
import sys
import traceback as tb
import warnings


# -----------------------------------------------------------------------------
def make_db2_tcfg(dbeng, obj):
    """
    Construct and return a config object for a db2 database
    """
    tcfg = CrawlConfig.CrawlConfig()
    xcfg = CrawlConfig.add_config('hpssic_mysql_test.cfg', close=True)
    section = 'dbi-hpss'
    tcfg.add_section(section)
    tcfg.set(section, 'dbtype', dbeng)
    tcfg.set(section, 'tbl_prefix', 'hpss')
    for optname in ['cfg', 'sub', 'dbtype', 'tbl_prefix',
                    'hostname', 'port', 'username', 'password']:
        tcfg.set(section, optname, xcfg.get(section, optname))
    CrawlConfig.add_config(cfg=tcfg, close=True)
    return tcfg


# -----------------------------------------------------------------------------
def make_mysql_tcfg(dbeng, obj):
    """
    Construct and return a config object for a mysql database
    """
    tcfg = CrawlConfig.CrawlConfig()
    xcfg = CrawlConfig.add_config(filename='hpssic_mysql_test.cfg', close=True)
    section = 'dbi-crawler'
    tcfg.add_section(section)
    tcfg.set(section, 'dbtype', dbeng)
    tcfg.set(section, 'dbname', xcfg.get('dbi-crawler', 'dbname'))
    tcfg.set(section, 'tbl_prefix', 'test')
    for dbparm in ['dbname', 'hostname', 'username', 'password']:
        tcfg.set(section, dbparm, xcfg.get(section, dbparm))
    CrawlConfig.add_config(cfg=tcfg, close=True)
    return tcfg


# -----------------------------------------------------------------------------
def make_sqlite_tcfg(dbeng, obj):
    """
    Construct and return a config object for an sqlite database
    """
    cdata = {'crawler': {'context': 'TEST',
                         'heartbeat': '10s',
                         'exitpath': '%s/TEST.exit' % MSG.default_piddir,
                         'stopwait_timeout': '5.0',
                         'sleep_time': '0.25',
                         'logpath': '/tmp/hpssic_sqlite.log',
                         'heartbeat': '10s',
                         },
             'dbi-crawler': {'dbtype': dbeng,
                             'dbname': obj.dbname(),
                             'tbl_prefix': 'test',
                             }
             }
    tcfg = CrawlConfig.add_config(close=True, dct=cdata)
    return tcfg


# -----------------------------------------------------------------------------
def make_tcfg(dbtype, obj):
    """
    Construct and return a config object suitable for a *dbtype* database by
    calling make_*dbtype*_tcfg()
    """
    func = getattr(sys.modules[__name__], 'make_%s_tcfg' % dbtype)
    rval = func(dbtype, obj)
    return rval


# -----------------------------------------------------------------------------
class DBITestRoot(testhelp.HelpedTestCase):
    # -------------------------------------------------------------------------
    def setup_select(self, table_name):
        """
        DBITestRoot:
        """
        self.reset_db(table_name)
        util.conditional_rm(self.dbname())
        db = self.DBI()
        db.create(table=table_name, fields=self.fdef)
        db.insert(table=table_name, fields=self.nk_fnames, data=self.testdata)
        return db

    # -------------------------------------------------------------------------
    def DBI(self, dbname='cfg'):
        """
        DBITestRoot: Return a CrawlDBI.DBI() object based on the current object
        """
        try:
            x = self._use_args
        except AttributeError:
            self._use_args = True

        if self._use_args:
            args = [make_tcfg(self.dbtype, self)]
            kw = {'dbtype': self.dbctype,
                  'timeout': 10.0}
        else:
            args = []
            kw = {'cfg': make_tcfg(self.dbtype, self),
                  'dbtype': self.dbctype,
                  'timeout': 10.0}

        if self.dbctype == 'hpss':
            kw['dbname'] = dbname
        rval = CrawlDBI.DBI(*args, **kw)
        self._use_args = not self._use_args
        return rval


# -----------------------------------------------------------------------------
class DBITest(DBITestRoot):
    """
    Tests for the DBI class
    """
    # -------------------------------------------------------------------------
    def test_ctor_nodbname(self):
        """
        DBITest: CrawlDBI ctor should not accept a dbname argument. It has to
        take its dbname from the config.

        CrawlDBI ctor called with a dbname but no dbtype should throw an
        exception.
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "dbtype must be 'hpss' or 'crawler' " +
                             "(dbname=None)",
                             CrawlDBI.DBI,
                             dbname='foobar')

    # -------------------------------------------------------------------------
    def test_ctor_pos0_arg(self):
        """
        DBITest: If the DBI ctor is called with something other than a config
        object in argv[0], it is expected to throw an exception
        """
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.unrecognized_arg_S % '.*?',
                               CrawlDBI.DBI,
                               'foobar')

    # -------------------------------------------------------------------------
    def test_ctor_bad_dbtype(self):
        """
        DBITest: If the DBI ctor is called with a dbtype other than one of the
        known ones, it is expected to throw an exception
        """
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.valid_dbtype,
                               CrawlDBI.DBI,
                               dbtype='Informix')

    # -------------------------------------------------------------------------
    def test_ctor_upd_closed(self):
        """
        DBITest: An attempt to update a table in a closed database should throw
        an exception.
        """
        a = CrawlDBI.DBI(cfg=make_tcfg('sqlite', self), dbtype='crawler')
        a.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             a.update,
                             table="foobar",
                             where="status = 'flappy'",
                             fields=['one', 'two', 'three'],
                             data=[('a', 'b', 'c'),
                                   ('x', 'y', 'z')])

    # -------------------------------------------------------------------------
    def test_ctor_sqlite(self):
        """
        DBITest: With a config object specifying sqlite as the database type,
        DBI should instantiate itself with an internal DBIsqlite object.

        It might seem like this test should be in the sqlite class below
        (DBIsqliteTest). However, this test is about verifying that the DBI
        class does the right thing based on the configuration it gets, not
        anything particular about sqlite. What's being tested here is not
        instantiating an sqlite database connection per se so much as veryfing
        that if the configuration says to get us an sqlite database connection,
        DBI doesn't hand us back a mysql connection.
        """
        a = CrawlDBI.DBI(cfg=make_tcfg('sqlite', self), dbtype='crawler')
        self.assertTrue(hasattr(a, '_dbobj'),
                        "Expected to find a _dbobj attribute on %s" % a)
        self.assertTrue(isinstance(a._dbobj, CrawlDBI.DBIsqlite),
                        "Expected %s to be a DBIsqlite object" % a._dbobj)


# -----------------------------------------------------------------------------
class DBI_in_Base(object):
    """
    Basic tests for the DBI<dbname> classes that do not change the database
    (i.e., these tests are run on sqlite, mysql, and db2).

    Arranging the tests this way allows us to avoid having to write a complete,
    independent set of tests for each database type.

    Class DBIsqliteTest, for example, which inherits the test methods from this
    one, will set the necessary parameters to select the sqlite database type
    so that when it is processed by the test running code, the tests will be
    run on an sqlite database. Similarly, DBImysqlTest will set the necessary
    parameters to select that database type, then run the inherited tests on a
    mysql database.
    """

    fdef = ['rowid integer primary key autoincrement',
            'name text',
            'size int',
            'weight double']
    fnames = [x.split()[0] for x in fdef]
    nk_fnames = fnames[1:]
    # tests below depend on testdata fulfilling the following conditions:
    #  * only one record with size = 92
    #  * only one record with name = 'zippo'
    testdata = [('frodo', 17, 108.5),
                ('zippo', 92, 12341.23),
                ('zumpy', 45, 9.3242),
                ('frodo', 23, 212.5),
                ('zumpy', 55, 90.6758)]

    # -------------------------------------------------------------------------
    def test_close(self):
        """
        DBI_in_Base: Calling close() should free up the db resources and make
        the database handle unusable.
        """
        a = self.DBI()
        a.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             a.table_exists, table='report')

    # -------------------------------------------------------------------------
    def test_close_closed(self):
        """
        DBI_in_Base: Closing a closed database should generate an exception.
        """
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.close)

    # -------------------------------------------------------------------------
    def test_close_deep(self):
        """
        DBI_in_Base: Closing a closed database should generate an exception.
        """
        self.dbgfunc()
        db = self.DBI()
        db.close()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.db_closed_already_rgx,
                               db._dbobj.close)

    # -------------------------------------------------------------------------
    def test_close_open(self):
        """
        DBI_in_Base: Closing an open database should work.
        """
        db = self.DBI()
        db.close()
        self.expected(True, db.closed)

    # -------------------------------------------------------------------------
    def test_closed_cursor(self):
        """
        DBI_in_Base: Calling cursor() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.cursor)

    # -------------------------------------------------------------------------
    def test_closed_describe(self):
        """
        DBI_in_Base: Calling describe() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.describe,
                             table='furby')

    # -------------------------------------------------------------------------
    def test_closed_select(self):
        """
        DBI_in_Base: Calling select() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.select,
                             table='furby',
                             fields=['one', 'two', 'three'])

    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        DBI_in_Base: Verify that a new object has the right attributes with the
        right default values
        """
        a = self.DBI()
        dirl = [q for q in dir(a) if not q.startswith('_')]
        xattr_req = ['alter', 'close', 'create', 'dbname', 'delete',
                     'describe', 'drop', 'closed',
                     'insert', 'select', 'table_exists', 'update', 'cursor']
        xattr_allowed = ['alter']

        for attr in dirl:
            if attr not in xattr_req and attr not in xattr_allowed:
                self.fail("Unexpected attribute %s on object %s" % (attr, a))
        for attr in xattr_req:
            if attr not in dirl:
                self.fail("Expected attribute %s not found on object %s" %
                          (attr, a))

    # -------------------------------------------------------------------------
    def test_ctor_bad_attrs(self):
        """
        DBI_in_Base: Attempt to create an object with an invalid attribute
        should get an exception
        """
        self.dbgfunc()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.invalid_attr_rgx,
                               CrawlDBI.DBI,
                               cfg=make_tcfg(self.dbtype, self),
                               badattr='frooble')

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_bad(self):
        """
        DBI_in_Base: With dbtype value other than 'hpss' or 'crawler',
        constructor should throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.valid_dbtype,
                             CrawlDBI.DBI,
                             dbtype='not-hpss-not-crawler')

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_none(self):
        """
        DBI_in_Base: Without dbtype, constructor should throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.valid_dbtype,
                             CrawlDBI.DBI)

    # -------------------------------------------------------------------------
    def test_describe_fail(self):
        """
        DBI_in_Base: Calling describe on a non-existent table should get an
        exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.no_such_table_desc_rgx,
                               db.describe,
                               table="frobble")
        db.close()

    # -------------------------------------------------------------------------
    def test_select_f(self):
        """
        DBI_in_Base: Calling select() specifying fields should get only the
        fields requested
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=['size', 'weight'])
        self.expected(2, len(rows[0]))
        for tup in self.testdata:
            self.assertTrue((tup[1], tup[2]) in rows,
                            "Expected %s in %s but it's not there" %
                            (str((tup[1], tup[2], )),
                             util.line_quote(str(rows))))

    # -------------------------------------------------------------------------
    def test_select_gb_f(self):
        """
        DBI_in_Base: Select with a group by clause on a field that is present
        in the table.
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=['sum(size)'], groupby='name')
        self.expected(3, len(rows))
        self.expected(True, ((40,)) in rows)
        self.expected(True, ((92,)) in rows)
        self.expected(True, ((100,)) in rows)

    # -------------------------------------------------------------------------
    def test_select_gb_ns(self):
        """
        DBI_in_Base: Select with a group by clause that is not a string --
        should get an exception.
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), groupby clause must be a string",
                             db.select,
                             table=tname,
                             fields=['sum(size)'],
                             groupby=['fiddle'])

    # -------------------------------------------------------------------------
    def test_select_gb_u(self):
        """
        DBI_in_Base: Select with a group by clause on a field that is unknown
        should get an exception.
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        ns_field = 'fiddle'

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ['no such column: %s' % ns_field,
                              "Unknown column '%s' in 'group statement'" %
                              ns_field],
                             db.select,
                             table=tname,
                             fields=['sum(size)'],
                             groupby=ns_field)

    # -------------------------------------------------------------------------
    def test_select_nq_mtd(self):
        """
        DBI_in_Base: Calling select() with where with no '?' and an empty data
        list is fine. The data returned should match the where clause.
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=self.nk_fnames,
                         where='size = 92', data=())
        self.expected(1, len(rows))
        self.expected(self.testdata[1], rows[0])

    # -------------------------------------------------------------------------
    def test_select_q_mtd(self):
        """
        DBI_in_Base: Calling select() with a where clause with a '?' and an
        empty data list should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        if self.dbtype == 'mysql':
            exctype = TypeError
            msg = "not enough arguments for format string"
        elif self.dbtype == 'sqlite':
            exctype = CrawlDBI.DBIerror
            msg = "Incorrect number of bindings supplied"

        self.assertRaisesMsg(exctype,
                             msg,
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             where='name = ?',
                             data=())

    # -------------------------------------------------------------------------
    def test_select_nq_ld(self):
        """
        DBI_in_Base: Calling select() with where clause with no '?' and data in
        the list should get an exception -- the data would be ignored
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Data would be ignored",
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             where="name = 'zippo'",
                             data=('frodo',))

    # -------------------------------------------------------------------------
    def test_select_q_ld(self):
        """
        DBI_in_Base: Calling select() with a where clause containing '?' and
        data in the data list should return the data matching the where clause
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=self.nk_fnames,
                         where='name = ?', data=('zippo',))
        self.expected(1, len(rows))
        self.expected([self.testdata[1], ], list(rows))

    # -------------------------------------------------------------------------
    def test_select_l_nint(self):
        """
        DBI_in_Base: select with limit not an int should throw exception
        """
        tname = util.my_name().replace("test_", "")
        self.setup_select(tname)
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), limit must be an int",
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             limit='this is a string')

    # -------------------------------------------------------------------------
    def test_select_l_int(self):
        """
        DBI_in_Base: select with int passed for *limit* should retrieve the
        specified number of records
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rlim = 3
        rows = db.select(table=tname, fields=self.nk_fnames, limit=rlim)
        self.expected(3, len(rows[0]))
        self.expected(rlim, len(rows))
        for tup in self.testdata[0:int(rlim)]:
            self.assertTrue(tup in rows,
                            "Expected %s in %s but it's not there" %
                            (str(tup), util.line_quote(rows)))

    # -------------------------------------------------------------------------
    def test_select_l_float(self):
        """
        DBI_in_Base: select with float passed for *limit* should convert the
        float to an int (without rounding) and use it
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rlim = 2.7
        rows = db.select(table=tname, fields=self.nk_fnames, limit=rlim)
        self.expected(3, len(rows[0]))
        self.expected(int(rlim), len(rows))
        for tup in self.testdata[0:int(rlim)]:
            self.assertTrue(tup in rows,
                            "Expected %s in %s but it's not there" %
                            (str(tup), util.line_quote(rows)))

    # -------------------------------------------------------------------------
    def test_select_mtf(self):
        """
        DBI_in_Base: Calling select() with an empty field list should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.wildcard_selects,
                             db.select,
                             table=tname,
                             fields=[])

    # -------------------------------------------------------------------------
    def test_select_mto(self):
        """
        DBI_in_Base: Calling select() with an empty orderby should get the data
        in the order inserted
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=self.nk_fnames, orderby='')
        self.expected(3, len(rows[0]))
        self.expected(list(self.testdata), list(rows))

    # -------------------------------------------------------------------------
    def test_select_mtt(self):
        """
        DBI_in_Base: Calling select() with an empty table name should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), table name must not be empty",
                             db.select,
                             table='',
                             fields=self.fnames)

    # -------------------------------------------------------------------------
    def test_select_mtw(self):
        """
        DBI_in_Base: Calling select() with an empty where arg should get all
        the data
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=self.nk_fnames, where='')
        self.expected(3, len(rows[0]))
        self.expected(list(self.testdata), list(rows))

    # -------------------------------------------------------------------------
    def test_select_nld(self):
        """
        DBI_in_Base: Calling select() with a non-tuple as the data argument
        should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), data must be a tuple",
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             where='name = ?',
                             data='zippo')

    # -------------------------------------------------------------------------
    def test_select_nlf(self):
        """
        DBI_in_Base: Calling select() with a non-list as the fields argument
        should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), fields must be a list",
                             db.select,
                             table=tname,
                             fields=17)

    # -------------------------------------------------------------------------
    def test_select_nso(self):
        """
        DBI_in_Base: Calling select() with a non-string orderby argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), orderby clause must be a string",
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             orderby=22)

    # -------------------------------------------------------------------------
    def test_select_nst(self):
        """
        DBI_in_Base: Calling select() with a non-string table argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), table name must be a string",
                             db.select,
                             table={},
                             fields=self.fnames)

    # -------------------------------------------------------------------------
    def test_select_nsw(self):
        """
        DBI_in_Base: Calling select() with a non-string where argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), where clause must be a string",
                             db.select,
                             table=tname,
                             fields=self.fnames,
                             where=22)

    # -------------------------------------------------------------------------
    def test_select_o(self):
        """
        DBI_in_Base: Calling select() specifying orderby should get the rows in
        the order requested
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        exp = [self.testdata[2], self.testdata[4], self.testdata[0],
               self.testdata[3], self.testdata[1]]

        rows = db.select(table=tname, fields=self.nk_fnames, orderby='weight')
        self.expected(3, len(rows[0]))
        self.expected(list(exp), list(rows))

    # -------------------------------------------------------------------------
    def test_select_w(self):
        """
        DBI_in_Base: Calling select() specifying where should get only the rows
        requested
        """
        tname = util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        exp = [self.testdata[0], self.testdata[2], self.testdata[3]]

        rows = db.select(table=tname, fields=self.nk_fnames, where="size < 50")
        self.expected(3, len(rows[0]))
        self.expected(list(exp), list(rows))

    # -------------------------------------------------------------------------
    def test_table_exists_yes(self):
        """
        DBI_in_Base: If table foo exists, db.table_exists(table='foo') should
        return True
        """
        tname = util.my_name().replace('test_', '')
        self.reset_db(tname)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.expected(True, db.table_exists(table=tname))

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        DBI_in_Base: If table foo does not exist, db.table_exists(table='foo')
        should return False
        """
        tname = util.my_name().replace('test_', '')
        self.reset_db(tname)
        db = self.DBI()
        self.expected(False, db.table_exists(table=tname))


# -----------------------------------------------------------------------------
class DBI_out_Base(object):
    """
    Basic tests for the DBI<dbname> classes -- methods that change the database
    (i.e., these tests are run for sqlite and mysql but not for db2).

    Arranging the tests this way allows us to avoid having to write a complete,
    independent set of tests for each database type.

    The mySql and sqlite test classes will inherit this one in addition to
    DBI_in_Base.
    """
    fdef = ['rowid integer primary key autoincrement',
            'name text',
            'size int',
            'weight double']
    fnames = [x.split()[0] for x in fdef]
    # tests below depend on testdata fulfilling the following conditions:
    #  * only one record with size = 92
    #  * only one record with name = 'zippo'
    testdata = [('frodo', 17, 108.5),
                ('zippo', 92, 12341.23),
                ('zumpy', 45, 9.3242),
                ('frodo', 23, 212.5),
                ('zumpy', 55, 90.6758)]

    # db.alter() tests
    #
    # Syntax:
    #    db.alter(table=<tabname>, addcol=<col desc>, pos='first|after <col>')
    #    db.alter(table=<tabname>, dropcol=<col name>)
    #
    # - sqlite does not support dropping columns
    #
    # - sqlite does not pay attention to the pos argument. The default location
    #   for adding a column in mysql is after the last existing column. This is
    #   also sqlite's behavior
    # -------------------------------------------------------------------------
    def test_alter_add_exists(self):
        """
        DBI_out_Base: Calling alter() to add an existing column should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # try to add an existing column
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["Duplicate column name 'size'",
                              "duplicate column name: size"
                              ],
                             db.alter,
                             table=tname,
                             addcol='size int')
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_add_injection(self):
        """
        DBI_out_Base: Calling alter() to add a column with injection should get
        an exception
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # try to add an existing column
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Invalid addcol argument",
                             db.alter,
                             table=tname,
                             addcol='size int; select * from somewhere')
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_add_ok(self):
        """
        DBI_out_Base: Calling alter() to add a column with valid syntax should
        work. With no pos argument, both mysql and sqlite should add the new
        column at the end
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()

        if self.dbtype == 'mysql':
            exp = ('comment', 5L, 'text')
        elif self.dbtype == 'sqlite':
            exp = (4, 'comment', 'text', 0, None, 0)
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_closed_db(self):
        """
        DBI_out_Base: Calling alter() on a database that has been closed should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.alter,
                             table=tname,
                             addcol="furple int")

    # -------------------------------------------------------------------------
    def test_alter_drop_injection(self):
        """
        DBI_out_Base: Calling alter() to drop a column with injection should
          get an exception mysql: exception on injection sqlite: exception on
          drop arg
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["Invalid dropcol argument",
                              "SQLite does not support dropping columns"
                              ],
                             db.alter,
                             table=tname,
                             dropcol="size; select * from other")
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_drop_nx(self):
        """
        DBI_out_Base: Calling alter() to drop a column that does not exist
        should get an exception mysql: exception on nx col sqlite: exception on
        drop arg
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["Can't DROP 'fripperty'; " +
                              "check that column/key exists",
                              "SQLite does not support dropping columns"
                              ],
                             db.alter,
                             table=tname,
                             dropcol="fripperty")
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_mt_add(self):
        """
        DBI_out_Base: Calling alter() with an empty add should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On alter(), addcol must not be empty",
                             db.alter,
                             table=tname,
                             addcol="")
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_mt_drop(self):
        """
        DBI_out_Base: Calling alter() with an empty add should get an exception
          sqlite: exception on drop argument mysql: exception on empty drop arg
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["On alter, dropcol must not be empty",
                              "SQLite does not support dropping columns"
                              ],
                             db.alter,
                             table=tname,
                             dropcol="")
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_mt_table(self):
        """
        DBI_out_Base: Calling alter() with no table name should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On alter(), table name must not be empty",
                             db.alter,
                             table="",
                             addcol="dragon")
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_no_action(self):
        """
        DBI_out_Base: Calling alter() with no action should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "ALTER requires an action",
                             db.alter,
                             table=tname)
        db.close()

    # -------------------------------------------------------------------------
    def test_alter_table_notstr(self):
        """
        DBI_out_Base: Calling alter() with a non-string table should get an
        exception
        """
        self.dbgfunc()
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.alter_table_string,
                             db.alter,
                             table=32,
                             addcol="size")
        db.close()

    # -------------------------------------------------------------------------
    def test_closed_create(self):
        """
        DBI_out_Base: Calling create() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.create,
                             table=tname,
                             fields=self.fdef)

    # -------------------------------------------------------------------------
    def test_closed_delete(self):
        """
        DBI_out_Base: Calling delete() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.delete,
                             table=tname,
                             where="5 = 3")

    # -------------------------------------------------------------------------
    def test_closed_drop(self):
        """
        DBI_out_Base: Calling drop() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.drop,
                             table=tname)

    # -------------------------------------------------------------------------
    def test_closed_insert(self):
        """
        DBI_out_Base: Calling insert() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.insert,
                             table=tname,
                             fields=['one', 'two'],
                             data=[])

    # -------------------------------------------------------------------------
    def test_closed_update(self):
        """
        DBI_out_Base: Calling update() on a closed database should get an
        exception
        """
        tname = util.my_name()
        db = self.DBI()
        db.close()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db_closed,
                             db.insert,
                             table=tname,
                             fields=['one', 'two'],
                             data=[])

    # -------------------------------------------------------------------------
    def test_create_already(self):
        """
        DBI_out_Base: Calling create() for a table that already exists should
        get an exception
        """
        self.dbgfunc()
        db = self.DBI()
        tname = util.my_name()
        flist = ['rowid integer primary key autoincrement',
                 'one text',
                 'two int']
        if db.table_exists(table=tname):
            db.drop(table=tname)
        db.create(table=tname,
                  fields=flist)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             [MSG.table_already_sqlite,
                              MSG.table_already_mysql],
                             db.create,
                             table=tname,
                             fields=flist)
        db.close()

    # -------------------------------------------------------------------------
    def test_create_mtf(self):
        """
        DBI_out_Base: Calling create() with an empty field list should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), fields must not be empty",
                             db.create,
                             table='nogood',
                             fields=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_create_mtt(self):
        """
        DBI_out_Base: Calling create() with an empty table name should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), table name must not be empty",
                             db.create,
                             table='',
                             fields=['abc text'])
        db.close()

    # -------------------------------------------------------------------------
    def test_create_nlf(self):
        """
        DBI_out_Base: Calling create() with a non-list as the fields argument
        should get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), fields must be a list",
                             db.create,
                             table='create_nlf',
                             fields='notdict')
        db.close()

    # -------------------------------------------------------------------------
    def test_create_table_notstr(self):
        """
        DBI_out_Base: Calling create() with a non-string as the table argument
        should get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.create_table_string,
                             db.create,
                             table=14.7,
                             fields=['foo', 'bar'])
        db.close()

    # -------------------------------------------------------------------------
    def test_create_yes(self):
        """
        DBI_out_Base: Calling create() with correct arguments should create the
        table
        """
        util.conditional_rm(self.dbname())
        db = self.DBI()
        if db.table_exists(table='create_yes'):
            db.drop(table='create_yes')
        db.create(table='create_yes',
                  fields=['rowid integer primary key autoincrement',
                          'one text',
                          'two int'])
        self.assertTrue(db.table_exists(table='create_yes'))

    # -------------------------------------------------------------------------
    def test_ctor_dbn_none(self):
        """
        DBI_out_Base: Attempt to create an object with no dbname should get an
        exception
        """
        tcfg = make_tcfg(self.dbtype, self)
        tcfg.remove_option(CrawlDBI.CRWL_SECTION, 'dbname')
        exp = "No option 'dbname' in section: '%s'" % CrawlDBI.CRWL_SECTION
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             exp,
                             CrawlDBI.DBI,
                             cfg=tcfg,
                             dbtype='crawler')

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_crawler(self):
        """
        DBI_out_Base: With dbtype value 'crawler', constructor should be okay
        """
        db = self.DBI()
        self.assertTrue(hasattr(db, "_dbobj"),
                        "%s: Expected attribute '_dbobj', not present" %
                        self.dbtype)
        self.assertTrue(hasattr(db, 'closed'),
                        "%s: Expected attribute 'closed', not present" %
                        self.dbtype)
        self.assertTrue(hasattr(db._dbobj, 'dbh'),
                        "%s: Expected attribute 'dbh', not present" %
                        self.dbtype)
        db.close()

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_crawler_dbname(self):
        """
        DBI_out_Base: With dbtype value 'crawler' and dbname constructor should
        throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.dbname_not_allowed,
                             CrawlDBI.DBI,
                             dbtype="crawler",
                             dbname="popeye")

    # -------------------------------------------------------------------------
    def test_dbschem_drop_table(self):
        """
        DBI_out_Base: dbschem.drop_table should return a failure message if the
        table does not exist, or drop the table if it does.
        """
        self.dbgfunc()
        tname = util.my_name()
        tcfg = make_tcfg(self.dbtype, self)
        db = self.DBI()

        rv = dbschem.drop_table(table=tname, cfg=tcfg)
        self.expected("Table '%s' does not exist" % tname, rv)

        db.create(table=tname, fields=self.fdef)
        self.assertTrue(db.table_exists(table=tname))

        rv = dbschem.drop_table(table=tname, cfg=tcfg)
        self.expected("Attempt to drop table '%s' was successful" % tname, rv)

    # -------------------------------------------------------------------------
    def test_dbschem_make_table(self):
        """
        DBI_out_Base: dbschem.make_table() should create the table if it does
        not already exist. If the table does exist, dbschem.make_table() should
        do nothing and return the string 'Already'.
        """
        tcfg = make_tcfg(self.dbtype, self)
        result = dbschem.make_table('tcc_data', cfg=tcfg)
        db = self.DBI()
        self.assertTrue(db.table_exists(table='tcc_data'))
        self.expected("Created", result)
        result = dbschem.make_table('tcc_data', cfg=tcfg)
        self.expected("Already", result)

    # -------------------------------------------------------------------------
    def test_delete_except(self):
        """
        DBI_out_Base: A delete from a table that does not exist should throw an
        exception
        """
        self.dbgfunc()
        (db, td) = self.delete_setup()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.no_such_table_del_rgx,
                               db.delete,
                               table="nonesuch",
                               where="name='sam'")
        db.close()

    # -------------------------------------------------------------------------
    def test_delete_nq_nd(self):
        """
        A delete with no '?' in the where clause and no data tuple is
        okay. The records deleted should match the where clause.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where="name='sam'")
        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        for r in td['rows'][0:1] + td['rows'][2:]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        self.assertFalse(td['rows'][1] in rows,
                         "%s should have been deleted" % (td['rows'][1],))

    # -------------------------------------------------------------------------
    def test_delete_q_nd(self):
        """
        DBI_out_Base: A delete with a '?' in the where clause and no data tuple
        should get an exception.
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Criteria are not fully specified",
                             db.delete,
                             table=td['tabname'],
                             where='name=?')

        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_nq_td(self):
        """
        DBI_out_Base: A delete with no '?' in the where clause and a non-empty
        data list should get an exception -- the data would be ignored.
        """
        (db, td) = self.delete_setup()

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Data would be ignored",
                             db.delete,
                             table=td['tabname'],
                             where='name=foo',
                             data=('meg',))

        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_q_td(self):
        """
        DBI_out_Base: A delete with a '?' in the where clause and a non-empty
        data list should delete the data matching the where clause.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where='name=?', data=('gertrude',))
        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        for r in td['rows'][0:-1]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        self.assertFalse(td['rows'][-1] in rows,
                         "%s should have been deleted" % (td['rows'][1],))

    # -------------------------------------------------------------------------
    def test_delete_mtt(self):
        """
        DBI_out_Base: A delete with an empty table name should throw an
        exception.
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), table name must not be empty",
                             db.delete,
                             table='',
                             where='name=?',
                             data=('meg',))

        rows = db.select(table=td['tabname'],
                         fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_mtw(self):
        """
        DBI_out_Base: A delete with an empty where clause should delete all the
        data.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'])
        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        self.expected(0, len(rows))

    # -------------------------------------------------------------------------
    def test_delete_ntd(self):
        """
        DBI_out_Base: A delete with a non-tuple data value should throw an
        exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), data must be a tuple",
                             db.delete,
                             table=td['tabname'],
                             where='name=?',
                             data='meg')

        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_nst(self):
        """
        DBI_out_Base: A delete with a non-string table name should throw an
        exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), table name must be a string",
                             db.delete,
                             table=32,
                             where='name=?',
                             data='meg')

        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_nsw(self):
        """
        DBI_out_Base: A delete with a non-string where argument should throw an
        exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), where clause must be a string",
                             db.delete,
                             table=td['tabname'],
                             where=[])

        rows = db.select(table=td['tabname'], fields=td['ifields'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_w(self):
        """
        DBI_out_Base: A delete with a valid where argument should delete the
        data matching the where
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where="name like 's%'")
        rows = db.select(table=td['tabname'], fields=['id', 'name', 'age'])
        db.close()

        for r in td['rows'][0:1] + td['rows'][3:]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        for r in td['rows'][1:3]:
            self.assertFalse(r in rows,
                             "%s should have been deleted" % (r,))

    # -------------------------------------------------------------------------
    def test_drop_table_empty(self):
        """
        DBI_out_Base: If *table* is the empty string, drop should throw an
        exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.drop_table_empty,
                             db.drop,
                             table="")
        db.close()

    # -------------------------------------------------------------------------
    def test_drop_table_nonesuch(self):
        """
        DBI_out_Base: If *table* does not exist, drop should throw an exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.no_such_table_drop_rgx,
                               db.drop,
                               table="nonesuch")
        db.close()

    # -------------------------------------------------------------------------
    def test_drop_table_notstr(self):
        """
        DBI_out_Base: If *table* is not a string, drop should throw an
        exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.drop_table_string,
                             db.drop,
                             table=17)
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_fnox(self):
        """
        DBI_out_Base: Calling insert on fields not in the table should get an
        exception
        """
        self.reset_db('fnox')
        db = self.DBI()
        db.create(table='fnox',
                  fields=['rowid integer primary key autoincrement',
                          'one text',
                          'two text'])
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["table test_fnox has no column named three",
                              "Unknown column 'three' in 'field list'"],
                             db.insert,
                             table='fnox',
                             fields=['one', 'two', 'three'],
                             data=[('abc', 'def', 99),
                                   ('aardvark', 'buffalo', 78)])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_mtd(self):
        """
        DBI_out_Base: Calling insert with an empty data list should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), data list must not be empty",
                             db.insert,
                             table='mtd',
                             fields=['one', 'two'],
                             data=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_mtf(self):
        """
        DBI_out_Base: Calling insert with an empty field list should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), fields list must not be empty",
                             db.insert,
                             table='mtd',
                             fields=[],
                             data=[(1, 2)])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_mtt(self):
        """
        DBI_out_Base: Calling insert with an empty table name should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), table name must not be empty",
                             db.insert,
                             table='',
                             fields=['one', 'two'],
                             data=[(1, 2)])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_nst(self):
        """
        DBI_out_Base: Calling insert with a non-string table name should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), table name must be a string",
                             db.insert,
                             table=32,
                             fields=['one', 'two'],
                             data=[(1, 2)])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_nlf(self):
        """
        DBI_out_Base: Calling insert with a non-list fields arg should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), fields must be a list",
                             db.insert,
                             table='nlf',
                             fields='froo',
                             data=[(1, 2)])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_nld(self):
        """
        DBI_out_Base: Calling insert with a non-list data arg should get an
        exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), data must be a list",
                             db.insert,
                             table='nlf',
                             fields=['froo', 'pizzazz'],
                             data={})
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_tnox(self):
        """
        DBI_out_Base: Calling insert on a non-existent table should get an
        exception
        """
        self.dbgfunc()
        mcfg = make_tcfg(self.dbtype, self)
        util.conditional_rm(self.dbname())
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["no such table: test_tnox",
                              "Table '%s.test_tnox' doesn't exist" %
                              mcfg.get(CrawlDBI.CRWL_SECTION, 'dbname')],
                             db.insert,
                             table='tnox',
                             fields=['one', 'two'],
                             data=[('abc', 'def'),
                                   ('aardvark', 'buffalo')])
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_yes(self):
        """
        DBI_out_Base: Calling insert with good arguments should put the data in
        the table
        """
        self.dbgfunc()
        tname = util.my_name().replace('test_', '')
        self.reset_db(tname)
        fdef = ['id integer primary key autoincrement',
                'name text',
                'size int']
        fnames = [x.split()[0] for x in fdef]
        testdata = [(1, 'sinbad', 54),
                    (2, 'zorro', 98)]

        db = self.DBI()
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)
        db.close()

        db = self.DBI()
        dbc = db.cursor()
        dbc.execute("""
        select * from test_insert_yes
        """)
        rows = dbc.fetchall()
        for tup in testdata:
            self.assertTrue(tup in rows,
                            "Expected data %s not found in table" % str(tup))
        db.close()

    # -------------------------------------------------------------------------
    def test_composite_primary_key_duplicate(self):
        """
        DBI_out_Base: An attempt to insert an existing record into a table with
        a composite primary key should fail. 'history' is such a table.
        """
        self.dbgfunc()
        db = self.DBI()
        dbschem.make_table('history')
        db.insert(table='history',
                  fields=['plugin', 'runtime', 'errors'],
                  data=[('tcc', 1398456437, 0),
                        ('migr', 1401910729, 0),
                        ('report', 1401910729, 0),
                        ('cv', 1401910729, 0),
                        ('purge', 1403125415, 0)])

        # should fail
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["columns plugin, runtime are not unique",
                              "1062: Duplicate entry"],
                             db.insert,
                             table='history',
                             fields=['plugin', 'runtime', 'errors'],
                             data=[('tcc', 1398456437, 0)])
        rows = db.select(table='history',
                         fields=['plugin', 'runtime', 'errors'])
        self.expected(5, len(rows))

        # should succeed
        db.insert(table='history',
                  fields=['plugin', 'runtime', 'errors'],
                  data=[('cv', 1398456437, 0)])
        rows = db.select(table='history',
                         fields=['plugin', 'runtime', 'errors'])
        self.expected(6, len(rows))

        # what if we insert three rows and one is a duplicate. Do the other two
        # get inserted or does the whole thing fail? >>> With ignore=True, the
        # duplicates are dropped and everything else is inserted. However, in
        # this situation, mysql throws a warning.
        with warnings.catch_warnings(record=True) as wlist:
            db.insert(table='history',
                      ignore=True,
                      fields=['plugin', 'runtime', 'errors'],
                      data=[('cv', 1598456437, 0),
                            ('tcc', 1423423425, 0),
                            ('report', 1401910729, 0),
                            ])
            w = util.pop0(wlist)
            self.assertEqual(None, w, "Unexpected warning: %s" % w)
        rows = db.select(table='history',
                         fields=['plugin', 'runtime', 'errors'])
        self.expected(8, len(rows))

    # -------------------------------------------------------------------------
    def test_update_f(self):
        """
        DBI_out_Base: Calling update() specifying fields should update the
        fields requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]

        self.reset_db(tname)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.nk_fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size'],
                  data=[(x[1], x[0]) for x in udata],
                  where='name = ?')
        r = db.select(table=tname, fields=self.nk_fnames)

        for idx, tup in enumerate(udata):
            exp = (udata[idx][0], udata[idx][1], self.testdata[idx][2])
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
        db.close()

    # -------------------------------------------------------------------------
    def test_update_qp(self):
        """
        DBI_out_Base: Calling update() specifying fields should update the
        fields requested. However, placeholders should not be quoted.
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]

        self.reset_db(tname)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.nk_fnames, data=self.testdata)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Parameter placeholders should not be quoted",
                             db.update,
                             table=tname,
                             fields=['size'],
                             data=[(x[1], x[0]) for x in udata],
                             where='name = "?"')
        db.close()

    # -------------------------------------------------------------------------
    def test_update_mtd(self):
        """
        DBI_out_Base: Calling update() with an empty data list should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), data must not be empty",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_update_mtf(self):
        """
        DBI_out_Base: Calling update() with an empty field list should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), fields must not be empty",
                             db.update,
                             table=tname,
                             fields=[],
                             data=self.testdata)
        db.close()

    # -------------------------------------------------------------------------
    def test_update_mtt(self):
        """
        DBI_out_Base: Calling update() with an empty table name should get an
        exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), table name must not be empty",
                             db.update,
                             table='',
                             fields=self.fnames,
                             data=self.testdata)
        db.close()

    # -------------------------------------------------------------------------
    def test_update_mtw(self):
        """
        DBI_out_Base: Calling update() with an empty where arg should update
        all the rows
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]

        self.reset_db(tname)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.nk_fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size'],
                  data=[(43,)],
                  where='')
        r = db.select(table=tname, fields=self.nk_fnames)

        for idx, tup in enumerate(udata):
            exp = (udata[idx][0], 43, self.testdata[idx][2])
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
        db.close()

    # -------------------------------------------------------------------------
    def test_update_nlf(self):
        """
        DBI_out_Base: Calling update() with a non-list as the fields argument
        should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), fields must be a list",
                             db.update,
                             table=tname,
                             fields=17,
                             data=self.testdata)
        db.close()

    # -------------------------------------------------------------------------
    def test_update_nld(self):
        """
        DBI_out_Base: Calling update() with a non-list data argument should get
        an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), data must be a list of tuples",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data='notalist')
        db.close()

    # -------------------------------------------------------------------------
    def test_update_nonesuch(self):
        """
        DBI_out_Base: Calling update() on a table that does not exist should
        get an exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.no_such_table_upd_rgx,
                               db.update,
                               table="nonesuch",
                               fields=self.fnames[1:],
                               data=self.testdata)
        db.close()

    # -------------------------------------------------------------------------
    def test_update_nst(self):
        """
        DBI_out_Base: Calling update() with a non-string table argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), table name must be a string",
                             db.update,
                             table=38,
                             fields=self.fnames,
                             data=self.testdata)
        db.close()

    # -------------------------------------------------------------------------
    def test_update_nsw(self):
        """
        DBI_out_Base: Calling update() with a non-string where argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), where clause must be a string",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data=self.testdata,
                             where=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_update_w(self):
        """
        DBI_out_Base: Calling update() specifying where should update only the
        rows requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]

        self.reset_db(tname)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.nk_fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size', 'weight'],
                  data=[(udata[2][1:])],
                  where="name = 'zumpy'")
        r = db.select(table=tname, fields=self.nk_fnames)

        explist = self.testdata[0:2] + udata[2:]
        for exp in explist:
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
        db.close()

    # -------------------------------------------------------------------------
    def delete_setup(self):
        """
        DBI_out_Base: Set up for a delete test
        """
        flist = ['id integer primary key', 'name text', 'age int']
        testdata = {'tabname': 'test_table',
                    'flist': flist,
                    'ifields': [x.split()[0] for x in flist],
                    'rows': [(1, 'bob', 32),
                             (2, 'sam', 17),
                             (3, 'sally', 25),
                             (4, 'meg', 19),
                             (5, 'gertrude', 95)]}
        self.reset_db(testdata['tabname'])
        db = self.DBI()
        db.create(table=testdata['tabname'],
                  fields=testdata['flist'])
        db.insert(table=testdata['tabname'],
                  fields=testdata['ifields'],
                  data=testdata['rows'])
        return (db, testdata)


# -----------------------------------------------------------------------------
class DBImysqlTest(DBI_in_Base, DBI_out_Base, DBITestRoot):
    dbtype = 'mysql'
    dbctype = 'crawler'
    pass

    # -------------------------------------------------------------------------
    @classmethod
    def setUpClass(cls):
        """
        DBImysqlTest:
        """
        CrawlConfig.add_config(filename="hpssic_mysql_test.cfg", close=True)
        dbschem.drop_tables_matching("test_%")

    # -------------------------------------------------------------------------
    @classmethod
    def tearDownClass(cls):
        """
        DBImysqlTest:
        """
        if not pytest.config.getvalue("keep"):
            dbschem.drop_tables_matching("test_%")

    # -------------------------------------------------------------------------
    def test_alter_add_after(self):
        """
        DBImysqlTest: Calling alter() to add a column with valid syntax should
        add the new column, honoring the *pos* argument.
        """
        self.dbgfunc()
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='after name')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()

        exp = ('comment', 3L, 'text')
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_add_first(self):
        """
        DBImysqlTest: Calling alter() to add a column with valid syntax should
        add the new column, honoring the *pos* argument
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='first')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()

        exp = ('comment', 1L, 'text')
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_add_mt_pos(self):
        """
        DBImysqlTest: Calling alter() to add a column with an empty pos
        argument should add the new column at the end of the row
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()

        exp = ('comment', 5L, 'text')
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_drop_ok(self):
        """
        DBImysqlTest: Calling alter() to drop a column with valid syntax should
        drop the column
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, dropcol='size')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()

        exp = ('size', 4L, 'int')
        self.assertFalse(exp in c,
                         "Not expecting '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_ctor_bad_attrs_mysql(self):
        """
        DBImysqlTest: Attempt to create an object with an invalid attribute
        should get an exception
        """
        self.dbgfunc()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.invalid_attr_rgx,
                               CrawlDBI.DBImysql,
                               cfg=make_tcfg(self.dbtype, self),
                               badattr='frooble')

    # -------------------------------------------------------------------------
    def test_ctor_dbname_none(self):
        """
        DBImysqlTest: The DBImysql ctor requires 'dbname' and 'tbl_prefix' as
        keyword arguments
        """
        self.dbgfunc()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.dbname_required,
                             CrawlDBI.DBImysql,
                             tbl_prefix='xyzzy')

    # -------------------------------------------------------------------------
    def test_ctor_tblpfx_none(self):
        """
        DBImysqlTest: The DBImysql ctor requires 'tbl_prefix' as
        keyword arguments
        """
        self.dbgfunc()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.tblpfx_required,
                             CrawlDBI.DBImysql,
                             dbname='crawler')

    # -------------------------------------------------------------------------
    def test_dbschem_alter_table(self):
        """
        DBImysqlTest: test dbschem.alter_table
         > adding a column should be successful,
         > dropping a column should be successful,
         > passing both addcol and dropcol in the same invocation should raise
           an exception
        """
        self.dbgfunc()
        tname = util.my_name()
        tcfg = make_tcfg(self.dbtype, self)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        rv = dbschem.alter_table(table=tname,
                                 addcol="missing int",
                                 pos="first",
                                 cfg=tcfg)
        self.expected("Successful", rv)
        z = db.describe(table=tname)
        self.assertTrue(any(['missing' in x for x in z]),
                        "Expected field 'missing' in %s" % repr(z))

        rv = dbschem.alter_table(table=tname,
                                 dropcol="missing",
                                 cfg=tcfg)
        self.expected("Successful", rv)
        z = db.describe(table=tname)
        self.assertFalse(any(['missing' in x for x in z]),
                         "Field 'missing' should not appear in %s" % repr(z))

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "addcol and dropcol are mutually exclusive",
                             dbschem.alter_table,
                             table=tname,
                             addcol="missing int",
                             dropcol="missing",
                             cfg=tcfg)
        db.close()

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        DBImysqlTest: Test repr on a mysql database object, both open and
        closed
        """
        exp = "DBImysql(dbname='hpssicccsornlgov_hpssic_prod')"
        a = self.DBI()
        self.expected(exp, repr(a))
        a.close()
        exp = "[closed]" + exp
        self.expected(exp, repr(a))

    # -------------------------------------------------------------------------
    def reset_db(self, name=''):
        """
        DBImysqlTest: If the named table exists, drop it. If *name* is empty,
        return without doing anything.
        """
        if name == '':
            return
        db = self.DBI()
        if db.table_exists(table=name):
            db.drop(table=name)
        db.close()


# -----------------------------------------------------------------------------
class DBIsqliteTest(DBI_in_Base, DBI_out_Base, DBITestRoot):
    dbtype = 'sqlite'
    dbctype = 'crawler'

    # -------------------------------------------------------------------------
    def test_alter_add_after(self):
        """
        DBIsqliteTest: Calling alter() to add a column with valid syntax should
        add column at end. Argument *pos* is ignored.
        """
        self.dbgfunc()
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='after name')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()
        exp = (4, 'comment', 'text', 0, None, 0)
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_add_first(self):
        """
        DBIsqliteTest: Calling alter() to add a column with valid syntax should
        add column at end. Argument *pos* is ignored.
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='first')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()
        exp = (4, 'comment', 'text', 0, None, 0)
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_add_mt_pos(self):
        """
        DBIsqliteTest: Calling alter() to add a column with an empty pos
        argument should ignore pos arg and add column at end.
        """
        tname = util.my_name().replace('test_', '')
        # create test table
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)

        # add column at specified location
        db.alter(table=tname, addcol='comment text', pos='')

        # verify new column and its location
        c = db.describe(table=tname)
        db.close()
        exp = (4, 'comment', 'text', 0, None, 0)
        self.assertTrue(exp in c,
                        "Expected '%s' in '%s'" % (exp, c))

    # -------------------------------------------------------------------------
    def test_alter_drop_ok(self):
        """
        DBIsqliteTest: Calling alter() to drop a column with valid syntax
        should get an unsupported exception since sqlite does not support this
        operation.
        """
        self.dbgfunc()
        tname = util.my_name().replace('test_', '')
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "SQLite does not support dropping columns",
                             db.alter,
                             table=tname,
                             dropcol="size")
        db.close()

    # -------------------------------------------------------------------------
    def test_ctor_bad_attrs_sqlite(self):
        """
        DBIsqliteTest: Attempt to create an object with an invalid attribute
        should get an exception
        """
        self.dbgfunc()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.invalid_attr_rgx,
                               CrawlDBI.DBIsqlite,
                               cfg=make_tcfg(self.dbtype, self),
                               badattr='frooble')

    # -------------------------------------------------------------------------
    def test_ctor_dbn_db(self):
        """
        DBIsqliteTest: File dbname exists and is a database file -- we will use
        it.
        """
        # first, we create a database file from scratch
        util.conditional_rm(self.dbname())
        tabname = util.my_name()
        dba = self.DBI()
        dba.create(table=tabname, fields=['field1 text'])
        dba.close()
        self.assertPathPresent(self.dbname())
        s = os.stat(self.dbname())
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.dbname())

        # now, when we try to access it, it should be there
        dbb = self.DBI()
        self.assertTrue(dbb.table_exists(table=tabname))
        dbb.close()
        self.assertPathPresent(self.dbname())
        s = os.stat(self.dbname())
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_dir(self):
        """
        DBIsqliteTest: File dbname exists and is a directory -- we throw an
        exception.
        """
        util.conditional_rm(self.dbname())
        os.mkdir(self.dbname(), 0777)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "unable to open database file",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype, self),
                             dbtype=self.dbctype)

    # -------------------------------------------------------------------------
    def test_ctor_dbn_empty(self):
        """
        DBIsqliteTest: File dbname exists and is empty -- we will use it as a
        database.
        """
        util.conditional_rm(self.dbname())
        util.touch(self.dbname())
        db = self.DBI()
        db.create(table='testtab',
                  fields=['rowid integer primary key autoincrement'])
        db.close()
        self.assertPathPresent(self.dbname())
        s = os.stat(self.dbname())
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_fifo(self):
        """
        DBIsqliteTest: File dbname exists and is a fifo -- we throw an
        exception
        """
        util.conditional_rm(self.dbname())
        os.mkfifo(self.dbname())
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "disk I/O error",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype, self),
                             dbtype=self.dbctype)

    # -------------------------------------------------------------------------
    def test_ctor_dbname_none(self):
        """
        DBIsqliteTest: Called with no dbname, constructor should throw
        exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.dbname_required,
                             CrawlDBI.DBIsqlite)

    # -------------------------------------------------------------------------
    def test_ctor_tblpfx_none(self):
        """
        DBIsqliteTest: Called with no tbl_prefix, constructor should throw
        exception
        """
        self.dbgfunc()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.tblpfx_required,
                             CrawlDBI.DBIsqlite,
                             dbname='crawler')

    # -------------------------------------------------------------------------
    def test_ctor_dbn_nosuch(self):
        """
        DBIsqliteTest: File dbname does not exist -- initializing a db
        connection to it should create it.
        """
        util.conditional_rm(self.dbname())
        db = self.DBI()
        db.close()
        self.assertPathPresent(self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sock(self):
        """
        DBIsqliteTest: File dbname is a socket -- we throw an exception
        """
        util.conditional_rm(self.dbname())
        sockname = self.tmpdir(util.my_name())
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(sockname)
        tcfg = make_tcfg(self.dbtype, self)
        tcfg.set(CrawlDBI.CRWL_SECTION, 'dbname', sockname)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "unable to open database file",
                             CrawlDBI.DBI,
                             cfg=tcfg,
                             dbtype=self.dbctype)

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_dir(self):
        """
        DBIsqliteTest: File dbname exists is a symlink. We should react to what
        the symlink points at. If it's a directory, we throw an exception.
        """
        # the symlink points at a directory
        util.conditional_rm(self.dbname() + '_xyz')
        os.mkdir(self.dbname() + '_xyz', 0777)
        os.symlink(os.path.basename(self.dbname() + '_xyz'), self.dbname())
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "unable to open database file",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype, self),
                             dbtype=self.dbctype)

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_empty(self):
        """
        DBIsqliteTest: File dbname exists and is a symlink pointing at an empty
        file. We use it.
        """
        # the symlink points at a directory
        util.conditional_rm(self.dbname())
        util.conditional_rm(self.dbname() + '_xyz')
        util.touch(self.dbname() + '_xyz')
        os.symlink(os.path.basename(self.dbname() + '_xyz'), self.dbname())
        db = self.DBI()
        db.create(table='testtab',
                  fields=['rowid integer primary key autoincrement'])
        db.close

        self.assertPathPresent(self.dbname() + '_xyz')
        s = os.stat(self.dbname())
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_nosuch(self):
        """
        DBIsqliteTest: File dbname exists and is a symlink pointing at a
        non-existent file. We create it.
        """
        # the symlink points at a non-existent file
        util.conditional_rm(self.dbname())
        util.conditional_rm(self.dbname() + '_xyz')
        os.symlink(os.path.basename(self.dbname() + '_xyz'), self.dbname())
        db = self.DBI()
        db.create(table='testtab',
                  fields=['rowid integer primary key autoincrement'])
        db.close

        self.assertPathPresent(self.dbname() + '_xyz')
        s = os.stat(self.dbname())
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_text(self):
        """
        DBIsqliteTest: File dbname exists and contains text. We should throw an
        exception
        """
        util.conditional_rm(self.dbname())
        f = open(self.dbname(), 'w')
        f.write('This is a text file, not a database file\n')
        f.close()

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "file is encrypted or is not a database",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype, self),
                             dbtype='crawler')

        os.unlink(self.dbname())

    # -------------------------------------------------------------------------
    def test_ctor_sqlite_dbnreq(self):
        """
        DBIsqliteTest: The DBIsqlite ctor requires 'dbname' and 'tbl_prefix' as
        keyword arguments
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "A database name is required",
                             CrawlDBI.DBIsqlite,
                             tbl_prefix='xyzzy')

    # -------------------------------------------------------------------------
    def test_ctor_sqlite_tbpreq(self):
        """
        DBIsqliteTest: The DBIsqlite ctor requires 'tbl_prefix' as keyword
        arguments
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "A table prefix is required",
                             CrawlDBI.DBIsqlite,
                             dbname='foobar')

    # -------------------------------------------------------------------------
    def test_ctor_sqlite_other(self):
        """
        DBIsqliteTest: The DBIsqlite ctor takes only 'dbname' and 'tbl_prefix'
        as keyword arguments
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Attribute 'something' is not valid",
                             CrawlDBI.DBIsqlite,
                             dbname='foobar',
                             tbl_prefix='xyzzy',
                             something='fribble')

    # -------------------------------------------------------------------------
    @pytest.mark.jenkins_fail
    @pytest.mark.slow
    def test_cvlib_lscos_populate(self):
        """
        Try running cv_sublib.lscos_populate(). It should create the lscos
        table and populate it with data. Only available when we have hpss.
        """
        self.dbgfunc()
        db = self.DBI()
        try:
            cv_sublib.lscos_populate()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))
        self.assertTrue(db.table_exists(table='lscos'),
                        "Expected table 'lscos' to be present")
        data = db.select(table="lscos",
                         fields=['count(*)'])
        self.assertTrue(0 != data[0][0],
                        "Expected data to be present in table 'lscos'")

    # -------------------------------------------------------------------------
    def test_dbschem_alter_table(self):
        """
        DBIsqliteTest: Test dbschem.alter_table
         - adding a column should work
         - dropping a column should get an unsupported exception
         - passing both addcol and dropcol should be a mutual exclusion
           exception
        """
        tname = util.my_name()
        tcfg = make_tcfg(self.dbtype, self)
        db = self.DBI()
        db.create(table=tname, fields=self.fdef)
        db.close()
        rv = dbschem.alter_table(table=tname,
                                 addcol="missing int",
                                 pos="first",
                                 cfg=tcfg)
        self.expected("Successful", rv)

        db = self.DBI()
        z = db.describe(table=tname)
        db.close()
        self.assertTrue(any(['missing' in x for x in z]),
                        "Expected field 'missing' in %s" % repr(z))

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "SQLite does not support dropping columns",
                             dbschem.alter_table,
                             table=tname,
                             dropcol="missing",
                             cfg=tcfg)

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "addcol and dropcol are mutually exclusive",
                             dbschem.alter_table,
                             table=tname,
                             addcol="missing int",
                             dropcol="missing",
                             cfg=tcfg)

    # -------------------------------------------------------------------------
    def test_err_handler(self):
        """
        Test the sqlite error handler method
        """
        self.dbgfunc()
        e = sqlite3.OperationalError("no such table: leapfrog")
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "no such table: leapfrog",
                             db._dbobj.err_handler,
                             e)

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        DBIsqliteTest: With a config object specifying sqlite as the database
        type, calling __repr__ on a DBI object should produce a representation
        that looks like a DBIsqlite object.
        """
        exp = "DBIsqlite(dbname='%s')" % self.dbname()
        a = self.DBI()
        self.expected(exp, repr(a))
        a.close()
        exp = "[closed]" + exp
        self.expected(exp, repr(a))

    # -------------------------------------------------------------------------
    def reset_db(self, name=''):
        """
        DBIsqliteTest: reset the database
        """
        util.conditional_rm(self.dbname())


# -----------------------------------------------------------------------------
@pytest.mark.slow
class DBIdb2Test(DBI_in_Base, DBITestRoot):
    dbctype = 'hpss'   # the function of the database
    dbtype = 'db2'     # which database engine it uses

    # -------------------------------------------------------------------------
    def test_alter_unsupported(self):
        """
        DBIdb2Test: alter is not supported for DB2. Calls to it should get an
        exception.
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "ALTER",
                             db.alter,
                             table="foo",
                             addcol="new")
        db.close()

    # -------------------------------------------------------------------------
    def test_ctor_bad_attrs_db2(self):
        """
        DBIdb2Test: Attempt to create an object with an invalid attribute
        should get an exception
        """
        self.dbgfunc()
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.invalid_attr_rgx,
                               CrawlDBI.DBIdb2,
                               cfg=make_tcfg(self.dbtype, self),
                               badattr='frooble')

    # -------------------------------------------------------------------------
    @pytest.mark.jenkins_fail
    def test_err_handler_db2(self):
        """
        DBIdb2Test: The DB2 err handler will accept *err* (an exception object)
        or *message* (a string). It should alwasy raise a CrawlDBI.DBIerror.
        """
        import ibm_db_dbi
        self.dbgfunc()
        testerrnum = 1438
        testmsg = "This is a test"
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             testmsg,
                             db._dbobj.err_handler,
                             testmsg)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             testmsg,
                             db._dbobj.err_handler,
                             Exception(testmsg))
        e = ibm_db_dbi.Error(testmsg)
        e.args = (testerrnum, testmsg)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "%d: %s" % (testerrnum, testmsg),
                             db._dbobj.err_handler,
                             e)
        db.close()

    # -------------------------------------------------------------------------
    def test_ctor_nouser_nopass_noconn(self):
        """
        DBIdb2Test: Called with no dbname, constructor should throw exception
        """
        self.dbgfunc()
        cfg = make_tcfg(self.dbtype, self)
        cfg.remove_option('dbi-hpss', 'username')
        cfg.remove_option('dbi-hpss', 'password')
        self.assertRaisesRegex(CrawlDBI.DBIerror,
                               MSG.password_missing_rgx,
                               CrawlDBI.DBI,
                               cfg,
                               dbtype=self.dbctype,
                               dbname='cfg')

    # -------------------------------------------------------------------------
    def test_ctor_dbname_none(self):
        """
        DBIdb2Test: Called with no dbname, constructor should throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.dbname_required,
                             CrawlDBI.DBIdb2)

    # -------------------------------------------------------------------------
    def test_ctor_tblpfx_none(self):
        """
        DBIdb2Test: Called with no tbl_prefix, constructor should throw
        exception
        """
        self.dbgfunc()
        make_tcfg(self.dbtype, self)
        db = CrawlDBI.DBIdb2(dbname="cfg")
        self.expected('hpss.', db.tbl_prefix)
        db.close()

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_hpss_no_dbname(self):
        """
        DBIdb2Test: With dbtype value 'hpss', no dbname, constructor should
        throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "With dbtype=hpss, dbname must be specified",
                             CrawlDBI.DBI,
                             dbtype='hpss')

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_hpss_bad_dbname(self):
        """
        DBIdb2Test: With dbtype value 'hpss', bad dbname, constructor should
        throw exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "dbname frobble not defined in the configuration",
                             CrawlDBI.DBI,
                             dbtype='hpss',
                             dbname='frobble')

    # -------------------------------------------------------------------------
    def test_ctor_dbtype_hpss_dbname_sub_ok(self):
        """
        DBIdb2Test: With dbtype value 'hpss', good dbname, constructor should
        be okay
        """
        db = self.DBI()
        self.assertTrue(hasattr(db, "_dbobj"),
                        "%s: Expected attribute '_dbobj', not present" %
                        self.dbtype)
        self.assertTrue(hasattr(db, 'closed'),
                        "%s: Expected attribute 'closed', not present" %
                        self.dbtype)
        self.assertFalse(db.closed,
                         "%s: Expected db.closed to be False" %
                         self.dbtype)
        self.assertTrue(hasattr(db._dbobj, 'dbh'),
                        "%s: Expected attribute 'dbh', not present" %
                        self.dbtype)
        db.close()

    # -------------------------------------------------------------------------
    def test_hexstr(self):
        """
        DBIdb2Test: Passing a string of binary values to hexstr(), hexstr_uq()
        should get back the corresponding string of hex digits.
        """
        val = ('\x08\xe1"n\xff\xd2\xd9\x11\x80\x14\x10\x00Z\xfau\xbf\xa2' +
               '\xf9aj6\xfd\xd0\x11\x93\xcb\x00\x00\x00\x00\x00\x04')
        exp = ("08E1226EFFD2D911801410005AFA75BFA2F9616A36FDD01193CB" +
               "000000000004")
        xqexp = "x'" + exp + "'"
        self.expected(exp, CrawlDBI.DBIdb2.hexstr_uq(val))
        self.expected(xqexp, CrawlDBI.DBIdb2.hexstr(val))

    # -------------------------------------------------------------------------
    def test_hexval(self):
        """
        DBIdb2Test: Passing a string of hex digits to hexval() should get back
        the corresponding binary string.
        """
        val = ("08E1226EFFD2D911801410005AFA75BFA2F9616A36FDD01193CB" +
               "000000000004")
        xval = "x" + val
        xqval = "x'" + val + "'"
        exp = ('\x08\xe1"n\xff\xd2\xd9\x11\x80\x14\x10\x00Z\xfau\xbf\xa2' +
               '\xf9aj6\xfd\xd0\x11\x93\xcb\x00\x00\x00\x00\x00\x04')
        self.expected(exp, CrawlDBI.DBIdb2.hexval(val))
        self.expected(exp, CrawlDBI.DBIdb2.hexval(xval))
        self.expected(exp, CrawlDBI.DBIdb2.hexval(xqval))
        self.expected(exp, CrawlDBI.DBIdb2.hexval(exp))

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        DBIdb2Test: With a config object specifying sqlite as the database
        type, calling __repr__ on a DBI object should produce a representation
        that looks like a DBIsqlite object.
        """
        exp = "DBIdb2(dbname='cfg')"
        a = self.DBI()
        self.expected(exp, repr(a))
        a.close()
        exp = "[closed]" + exp
        self.expected(exp, repr(a))

    # -------------------------------------------------------------------------
    def test_select_f(self):
        """
        DBIdb2Test: Calling select() specifying fields should get only the
        fields requested
        """
        db = self.DBI()
        rows = db.select(table='hpss.cos',
                         fields=['cos_id', 'hier_id'])
        self.expected(2, len(rows[0].keys()))
        for exp in ['HIER_ID', 'COS_ID']:
            self.assertTrue(exp in rows[0].keys(),
                            "Expected key '%s' in each row, not found" %
                            exp)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_gb_f(self):
        """
        DBIdb2Test: Select with a group by clause on a field that is present in
        the table.
        """
        db = self.DBI(dbname='sub')
        rows = db.select(table='hpss.bitfile',
                         fields=['max(bfid) as mbf',
                                 'bfattr_cos_id'],
                         groupby='bfattr_cos_id')
        self.expected(2, len(rows[0].keys()))
        for exp in ['MBF', 'BFATTR_COS_ID']:
            self.assertTrue(exp in rows[0].keys(),
                            "Expected key '%s' in each row, not found" %
                            exp)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_gb_ns(self):
        """
        DBIdb2Test: Select with a group by clause that is not a string --
        should get an exception.
        """
        db = self.DBI(dbname='sub')
        exp = "On select(), groupby clause must be a string"
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             exp,
                             db.select,
                             table='hpss.bitfile',
                             fields=['max(bfid) as mbf',
                                     'bfattr_cos_id'],
                             groupby=17)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_gb_u(self):
        """
        DBIdb2Test: Select with a group by clause on a field that is unknown
        should get an exception.
        """
        self.dbgfunc()
        db = self.DBI(dbname='sub')
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             '"UNKNOWN_FIELD" is not valid in the context ' +
                             'where it',
                             db.select,
                             table="hpss.bitfile",
                             fields=['max(bfid) as mbf',
                                     'bfattr_cos_id'],
                             groupby='unknown_field')
        db.close()

    # -------------------------------------------------------------------------
    def test_select_join(self):
        """
        DBIdb2Test: Select should support joining tables.
        """
        db = self.DBI(dbname='sub')
        rows = db.select(table=['nsobject', 'bitfile'],
                         fields=['object_id',
                                 'name',
                                 'bitfile_id',
                                 'bfattr_cos_id'],
                         where="bitfile_id = bfid")
        self.assertTrue(0 < len(rows),
                        "Expected at least one row, got 0")
        db.close()

    # -------------------------------------------------------------------------
    def test_select_join_tn(self):
        """
        DBIdb2Test: Select should support joining tables with temporary table
        names.
        """
        db = self.DBI(dbname='sub')
        rows = db.select(table=['nsobject A', 'bitfile B'],
                         fields=['A.object_id',
                                 'A.name',
                                 'A.bitfile_id',
                                 'B.bfattr_cos_id'],
                         where="A.bitfile_id = B.bfid")
        self.assertTrue(0 < len(rows),
                        "Expected at least one row, got 0")
        db.close()

    # -------------------------------------------------------------------------
    def test_select_l_nint(self):
        """
        DBIdb2Test: select with limit not an int should throw an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), limit must be an int",
                             db.select,
                             table="cartridge",
                             fields=['cart',
                                     'version',
                                     'cart_sides'],
                             limit='not an int')
        db.close()

    # -------------------------------------------------------------------------
    def test_select_l_int(self):
        """
        DBIdb2Test: select with limit being an int should retrieve the
        indicated number of records
        """
        db = self.DBI(dbname='sub')
        rlim = 3
        rows = db.select(table='hpss.nsobject',
                         fields=["object_id",
                                 "name",
                                 "bitfile_id"],
                         limit=rlim)
        self.expected(rlim, len(rows))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_l_float(self):
        """
        DBIdb2Test: select with limit being a float should convert the value to
        an int (without rounding) and retrieve that number of records
        """
        db = self.DBI(dbname='sub')
        rlim = 4.5
        rows = db.select(table='hpss.nsobject',
                         fields=["object_id",
                                 "name",
                                 "bitfile_id"],
                         limit=rlim)
        self.expected(int(rlim), len(rows))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_mtf(self):
        """
        DBIdb2Test: Calling select() with an empty field list should get an
        exception -- an empty field list indicates the wildcard option
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.wildcard_selects,
                             db.select,
                             table="hpss.cos",
                             fields=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nf(self):
        """
        DBIdb2Test: Calling select() with no field list should get an exception
        -- fields should default to the empty list, indicating the wildcard
        option, which is not supported
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.wildcard_selects,
                             db.select,
                             table='hpss.pvlpv')
        db.close()

    # -------------------------------------------------------------------------
    def test_select_mto(self):
        """
        DBIdb2Test: Calling select() with an empty orderby should get the data
        in the same order as using no orderby at all.
        """
        db = self.DBI(dbname='sub')
        ordered_rows = db.select(table='hpss.bitfile',
                                 fields=['bfid'],
                                 orderby='')
        unordered_rows = db.select(table='hpss.bitfile',
                                   fields=['bfid'])
        okl = [CrawlDBI.DBIdb2.hexstr(x['BFID'])
               for x in ordered_rows]
        ukl = [CrawlDBI.DBIdb2.hexstr(x['BFID'])
               for x in unordered_rows]
        self.expected(ukl, okl)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_mtt(self):
        """
        DBIdb2Test: Calling select() with an empty table name should get an
        exception
        """
        db = self.DBI()
        exp = "On select(), table name must not be empty"
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             exp,
                             db.select,
                             table='',
                             fields=['max(desc_name) as mdn',
                                     'log_record_type_mask'],
                             groupby='unknown_field')
        db.close()

    # -------------------------------------------------------------------------
    def test_select_mtw(self):
        """
        DBIdb2Test: Calling select() with an empty where arg should get the
        same data as no where arg at all
        """
        db = self.DBI(dbname='sub')
        flist = ['object_id', 'name', 'bitfile_id']
        w_rows = db.select(table='hpss.nsobject', fields=flist, where='')
        x_rows = db.select(table='hpss.nsobject', fields=flist)
        self.expected(len(x_rows), len(w_rows))
        for exp, actual in zip(x_rows, w_rows):
            self.expected(actual, exp)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nld(self):
        """
        DBIdb2Test: Calling select() with a non-tuple as the data argument
        should get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), data must be a tuple",
                             db.select,
                             table="hpss.bitfile",
                             fields=self.fnames,
                             where="desc_name = ?",
                             data='prudhoe')
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nlf(self):
        """
        DBIdb2Test: Calling select() with a non-list as the fields argument
        should get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), fields must be a list",
                             db.select,
                             table="hpss.bitfile",
                             fields=92,
                             where="desc_name = ?",
                             data=('prudhoe', ))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nq_ld(self):
        """
        DBIdb2Test: Calling select() with where clause with no '?' and data in
        the list should get an exception -- the data would be ignored
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Data would be ignored",
                             db.select,
                             table="hpss.bitfile",
                             fields=self.fnames,
                             where="desc_name = ''",
                             data=('prudhoe', ))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nq_mtd(self):
        """
        DBIdb2Test: Calling select() with where with no '?' and an empty data
        list is fine. The data returned should match the where clause.
        """
        db = self.DBI(dbname='sub')
        crit = 'logfile'
        rows = db.select(table='hpss.nsobject',
                         fields=['name', 'object_id', 'bitfile_id'],
                         where="name like '%%%s%%'" % crit,
                         data=())
        for x in rows:
            self.assertTrue(crit in x['NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['NAME']))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nso(self):
        """
        DBIdb2Test: Calling select() with a non-string orderby argument should
        get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), orderby clause must be a string",
                             db.select,
                             table="hpss.bitfile",
                             fields=['bfid'],
                             orderby=22)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nst(self):
        """
        DBIdb2Test: Calling select() with a non-string table argument should
        get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), table name must be a string",
                             db.select,
                             table=47,
                             fields=['desc_name'],
                             orderby=22)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_nsw(self):
        """
        DBIdb2Test: Calling select() with a non-string where argument should
        get an exception
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On select(), where clause must be a string",
                             db.select,
                             table="hpss.nsobject",
                             fields=['name',
                                     'object_id',
                                     'bitfile_id'],
                             where=[])
        db.close()

    # -------------------------------------------------------------------------
    def test_select_o(self):
        """
        DBIdb2Test: Calling select() specifying orderby should get the rows in
        the order requested
        """
        db = self.DBI(dbname='sub')
        ordered_rows = db.select(table='hpss.bitfile',
                                 fields=['bfid'],
                                 orderby='bfid')

        ford = [CrawlDBI.DBIdb2.hexstr(x['BFID'])
                for x in ordered_rows]

        sord = sorted([CrawlDBI.DBIdb2.hexstr(x['BFID'])
                       for x in ordered_rows])

        self.expected(sord, ford)
        db.close()

    # -------------------------------------------------------------------------
    def test_select_q_ld(self):
        """
        DBIdb2Test: Calling select() with a where clause containing '?' and
        data in the data list should return the data matching the where clause
        """
        db = self.DBI(dbname='sub')
        crit = 'logfile'
        rows = db.select(table='hpss.nsobject',
                         fields=['name', 'object_id', 'bitfile_id'],
                         where="name like '%?%'",
                         data=(crit,))
        for x in rows:
            self.assertTrue(crit in x['NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['NAME']))
        db.close()

    # -------------------------------------------------------------------------
    def test_select_q_mtd(self):
        """
        DBIdb2Test: Calling select() with a where clause with a '?' and an
        empty data list should get an exception
        """
        self.dbgfunc()
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "0 params bound not matching 1 required",
                             db.select,
                             table="hpss.bitfile",
                             fields=['bfid'],
                             data=(),
                             where="DESC_NAME = ?")
        db.close()

    # -------------------------------------------------------------------------
    def test_select_w(self):
        """
        DBIdb2Test: Calling select() specifying where should get only the rows
        requested
        """
        db = self.DBI(dbname='sub')
        crit = 'logfile'
        rows = db.select(table='hpss.nsobject',
                         fields=['name', 'object_id', 'bitfile_id'],
                         where="name like '%%%s%%'" % crit)
        for x in rows:
            self.assertTrue(crit in x['NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['NAME']))
        db.close()

    # -------------------------------------------------------------------------
    def test_table_exists_yes(self):
        """
        DBIdb2Test: For a table that exists, table_exists() should return True.
        """
        db = self.DBI()
        self.expected(True, db.table_exists(table='cartridge'))
        db.close()

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        DBIdb2Test: For a table that does not exist, table_exists() should
        return False.
        """
        db = self.DBI()
        self.expected(False, db.table_exists(table='nonesuch'))
        db.close()

    # -------------------------------------------------------------------------
    def test_insert_exception(self):
        """
        DBIdb2Test: On a db2 database, insert should throw an exception.
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "INSERT",
                             db.insert,
                             table="hpss.bogus",
                             data=[('a', 'b', 'c')])
        db.close()

    # -------------------------------------------------------------------------
    def test_create_exception(self):
        """
        DBIdb2Test: On a db2 database, create should throw an exception.
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "CREATE",
                             db.create,
                             table="hpss.nonesuch",
                             fields=self.fdef)
        db.close()

    # -------------------------------------------------------------------------
    def test_delete_exception(self):
        """
        DBIdb2Test: On a db2 database, delete should throw an exception.
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "DELETE",
                             db.delete,
                             table="hpss.bogus",
                             data=[('a',)],
                             where="field = ?")
        db.close()

    # -------------------------------------------------------------------------
    def test_drop_exception(self):
        """
        DBIdb2Test: On a db2 database, drop should throw an exception.
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "DROP",
                             db.drop,
                             table="hpss.bogus")
        db.close()

    # -------------------------------------------------------------------------
    def test_update_exception(self):
        """
        DBIdb2Test: On a db2 database, update should throw an exception.
        """
        db = self.DBI()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             MSG.db2_unsupported_S % "UPDATE",
                             db.update,
                             table="hpss.bogus",
                             fields=['one', 'two'],
                             data=[('a', 'b', 'c')],
                             where="one = ?")
        db.close()
