"""
Tests for util.py
"""
import copy
import hx.cfg
import logging
import os
import pdb
import random
import re
import stat
import sys
import hx.testhelp
import time
import hx.util
import hx.util as U


# -----------------------------------------------------------------------------
class UtilTest(hx.testhelp.HelpedTestCase):
    """
    Tests for util.py
    """
    # -------------------------------------------------------------------------
    def test_Chdir(self):
        """
        U.Chdir() is a context manager. Upon entry to the 'with' clause, the
        CM sets the current directory to the argument passed to Chdir(). Upon
        exit from the 'with' clause, the CM returns to the original directory.
        """
        self.dbgfunc()
        start = os.getcwd()
        with U.Chdir("/tmp"):
            self.expected("/tmp", os.getcwd())
        self.expected(start, os.getcwd())

    # -------------------------------------------------------------------------
    def test_csv_list(self):
        """
        csv_list() called with whitespace should return an empty list
        csv_list() on whitespace with a comma in it => ['', '']
        csv_list() on 'a, b   , c' => ['a', 'b', 'c']
        """
        self.dbgfunc()
        self.expected([], U.csv_list(""))
        self.expected([], U.csv_list("     "))
        self.expected(['', ''], U.csv_list("  , "))
        self.expected(['xyz'], U.csv_list("xyz"))
        self.expected(['abc', ''], U.csv_list("  abc, "))
        self.expected(['a', 'b', 'c'], U.csv_list(" a,b ,  c  "))

    # -------------------------------------------------------------------------
    def test_content_list(self):
        """
        contents() reads and returns the contents of a file as a list
        """
        self.dbgfunc()
        x = U.contents(U.__file__.replace(".pyc", ".py"), string=False)
        self.expected(type(x), list)
        self.expected_in('def contents\(', x)

    # -------------------------------------------------------------------------
    def test_content_str(self):
        """
        contents() is supposed to read and return the contents of a file as a
        string.
        """
        self.dbgfunc()
        x = U.contents(U.__file__.replace(".pyc", ".py"))
        self.expected(str, type(x))
        self.expected_in('def contents\(', x)

    # -------------------------------------------------------------------------
    def test_date_end(self):
        """
        Given a file containing several log records, return the timestamp on
        the last one.
        """
        self.dbgfunc()
        tdata = ["This line should be ignored\n",
                 "2014.0412 12:25:50 This is not the timestamp to return\n",
                 "2014.0430 19:30:00 This should not be returned\n",
                 "2014.0501 19:30:00 Return this one\n",
                 "We want plenty of data here at the end of the file\n",
                 "with no timestamp so we'll be forced to read\n",
                 "backward a time or two, not just find the timestamp\n",
                 "on the first read so we exercise revread.\n"]
        tfilename = self.tmpdir("%s.data" % U.my_name())
        f = open(tfilename, 'w')
        f.writelines(tdata)
        f.close()

        self.expected("2014.0501", U.date_end(tfilename))

    # -------------------------------------------------------------------------
    def test_date_start(self):
        """
        Given a file containing several log records (with some irrelevant
        introductory material), return the timestamp on the first one.
        """
        self.dbgfunc()
        tdata = ["This line should be ignored\n",
                 "2014.0412 12:25:50 This is the timestamp to return\n",
                 "2014.0430 19:30:00 This should not be returned\n"]
        tfilename = self.tmpdir("%s.data" % (U.my_name()))
        f = open(tfilename, 'w')
        f.writelines(tdata)
        f.close()

        self.expected("2014.0412", U.date_start(tfilename))

    # -------------------------------------------------------------------------
    def test_day_offset(self):
        """
        The routine day_offset(arg) returns the epoch time of the beginning of
        the day (i.e., midnight) that is *args* days from today.
        """
        def validate(days):
            daylen = 24 * 3600
            low = U.daybase(time.time()) + days * daylen
            high = low + daylen
            r = U.day_offset(days) + 30   # 30 seconds into the day
            self.assertTrue(low <= r and r < high,
                            "Expected %s <= %s < %s" %
                            (U.ymdhms(low), U.ymdhms(r), U.ymdhms(high)))
        self.dbgfunc()
        validate(-2)
        validate(-1)
        validate(0)
        validate(1)
        validate(2)

    # -------------------------------------------------------------------------
    def test_env_add_folded_none(self):
        """
        TEST: add to an undefined environment variable from a folded [env]
        entry

        EXP: the value gets set to the payload with the whitespace squeezed out
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        add = "four:\n   five:\n   six"
        exp = re.sub("\n\s*", "", add)

        # make sure the target env variable is not defined
        with U.tmpenv(evname, None):
            # create a config object with an 'env' section and a '+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, '+' + add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the variable was set to the expected value
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_add_folded_pre(self):
        """
        TEST: add to a preset environment variable from a folded [env]
        entry

        EXP: the value gets set to the payload with the whitespace squeezed out
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:\n   five:\n   six"
        exp = ":".join([pre_val, re.sub("\n\s*", "", add)])

        # make sure the target env variable has the expected value
        with U.tmpenv(evname, pre_val):
            # create a config object with an 'env' section and a folded '+'
            # option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, '+' + add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the variable was set to the expected value
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_add_none(self):
        """
        TEST: add to an undefined environment variable from [env] entry

        EXP: the value gets set to the payload
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        add = "four:five:six"
        exp = add

        # make sure the target env variable is not defined
        with U.tmpenv(evname, None):
            # create a config object with an 'env' section and a '+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, '+' + add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the variable was set to the expected value
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_add_pre(self):
        """
        TEST: add to a predefined environment variable from [env] entry

        EXP: payload is appended to the old value
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:five:six"
        exp = ":".join([pre_val, add])

        # make sure the target env variable is set to a known value
        with U.tmpenv(evname, pre_val):
            # create a config object with an 'env' section and a '+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, "+" + add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the target env variable now contains both old and
            # added values
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_set_folded_none(self):
        """
        TEST: set undefined environment variable from a folded [env] entry
        unconditionally

        EXP: the value gets set
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        newval = "one:\n   two:\n   three"
        exp = re.sub("\n\s*", "", newval)

        # make sure the target env variable is not defined
        with U.tmpenv(evname, None):
            # create a config object with an 'env' section and a non-'+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, newval)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the variable was set to the expected value
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_set_pre_folded(self):
        """
        TEST: set predefined environment variable from a folded [env] entry
        unconditionally

        EXP: the old value gets overwritten
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:\n   five:\n   six"
        exp = re.sub("\n\s*", "", add)

        # make sure the target env variable is set to a known value
        with U.tmpenv(evname, pre_val):
            # create a config object with an 'env' section and a non-'+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the target env variable now contains the new value
            # and the old value is gone
            self.expected(exp, os.environ[evname])
            self.assertTrue(pre_val not in os.environ[evname],
                            "The old value should be gone but still seems " +
                            "to be hanging around")

    # -------------------------------------------------------------------------
    def test_env_set_none(self):
        """
        TEST: set undefined environment variable from [env] entry
        unconditionally

        EXP: the value gets set
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        exp = "newval"

        # make sure the target env variable is not defined
        with U.tmpenv(evname, None):
            # create a config object with an 'env' section and a non-'+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, exp)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the variable was set to the expected value
            self.expected(exp, os.environ[evname])

    # -------------------------------------------------------------------------
    def test_env_set_pre(self):
        """
        TEST: set predefined environment variable from [env] entry
        unconditionally

        EXP: the old value gets overwritten
        """
        self.dbgfunc()
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:five:six"
        exp = add

        # make sure the target env variable is set to a known value
        with U.tmpenv(evname, pre_val):
            # create a config object with an 'env' section and a non-'+' option
            cfg = hx.cfg.config()
            cfg.add_section(sname)
            cfg.set(sname, evname, add)

            # pass the config object to U.env_update()
            U.env_update(cfg)

            # verify that the target env variable now contains the new value
            # and the old value is gone
            self.expected(exp, os.environ[evname])
            self.assertTrue(pre_val not in os.environ[evname],
                            "The old value should be gone but still seems " +
                            "to be hanging around")

    # -------------------------------------------------------------------------
    def test_epoch(self):
        """
        U.epoch() converts a date string matching one of a list of formats
        to an epoch time.
        """
        self.dbgfunc()
        self.expected(1388638799, U.epoch("2014.0101 23:59:59"))
        self.expected(1388638799, U.epoch("2014.0101.23.59.59"))
        self.expected(1388638740, U.epoch("2014.0101 23:59"))
        self.expected(1388638740, U.epoch("2014.0101.23.59"))
        self.expected(1388635200, U.epoch("2014.0101 23"))
        self.expected(1388635200, U.epoch("2014.0101.23"))
        self.expected(1388552400, U.epoch("2014.0101"))
        self.expected(1388552399, U.epoch("1388552399"))
        self.assertRaisesMsg(U.HXerror,
                             "The date '' does not match any of the formats:",
                             U.epoch,
                             "")

    # -------------------------------------------------------------------------
    def test_expand_unset(self):
        """
        Test expanding an unset variable with simple, braced, and default
        syntaxes
        """
        self.dbgfunc()
        vname = "EXPAND_UNSET"
        with U.tmpenv(vname, None):
            bare_str = "before  $%s   after" % vname
            exp = "before     after"
            actual = U.expand(bare_str)
            self.expected(exp, actual)

            braced_str = "before/${%s}/after" % vname
            exp = "before//after"
            actual = U.expand(braced_str)
            self.expected(exp, actual)

            def_str = "before.${%s:-default-value}.after" % vname
            exp = "before.default-value.after"
            actual = U.expand(def_str)
            self.expected(exp, actual)

    # -------------------------------------------------------------------------
    def test_expand_empty(self):
        """
        Test expanding an empty variable with simple, braced, and default
        syntaxes
        """
        self.dbgfunc()
        vname = "EXPAND_EMPTY"
        with U.tmpenv(vname, ""):
            bare_str = "before  $%s   after" % vname
            exp = "before     after"
            actual = U.expand(bare_str)
            self.expected(exp, actual)

            braced_str = "before/${%s}/after" % vname
            exp = "before//after"
            actual = U.expand(braced_str)
            self.expected(exp, actual)

            def_str = "before.${%s:-default-value}.after" % vname
            exp = "before..after"
            actual = U.expand(def_str)
            self.expected(exp, actual)

    # -------------------------------------------------------------------------
    def test_expand_filled(self):
        """
        Test expand on

            'before $VAR after',
            'before/${VAR}/after',
            'before.${VAR:-default-value}.after'

        Note that the default value is allowed but not used in the expansion.
        Python does not natively support that aspect of shell variable
        expansion.
        """
        self.dbgfunc()
        vname = "EXPAND_FILLED"
        with U.tmpenv(vname, "SOMETHING"):
            bare_str = "before  $%s   after" % vname
            exp = "before  SOMETHING   after"
            actual = U.expand(bare_str)
            self.expected(exp, actual)

            braced_str = "before/${%s}/after" % vname
            exp = "before/SOMETHING/after"
            actual = U.expand(braced_str)
            self.expected(exp, actual)

            def_str = "before.${%s:-default-value}.after" % vname
            exp = "before.SOMETHING.after"
            actual = U.expand(def_str)
            self.expected(exp, actual)

    # -------------------------------------------------------------------------
    def test_git_repo(self):
        """
        Make sure git_repo() works on relative paths
        """
        self.dbgfunc()
        tdir = U.dirname(__file__)   # .../hx/dev/tests
        gdir = U.dirname(tdir)       # .../hx/dev
        odir = U.dirname(gdir)       # .../hx
        exp = gdir
        self.expected(exp, U.git_repo('.'))
        self.expected(exp, U.git_repo(__file__))
        self.expected(exp, U.git_repo(tdir))
        self.expected(exp, U.git_repo(gdir))
        self.expected('', U.git_repo(odir))
        self.expected('', U.git_repo(U.dirname(odir)))

    # -------------------------------------------------------------------------
    def test_hostname_default(self):
        """
        !@! could use unexpected_in
        Calling U.hostname() with no argument should get the short hostname
        """
        self.dbgfunc()
        hn = U.hostname()
        self.assertFalse('.' in hn,
                         "Short hostname expected but got '%s'" % hn)

    # -------------------------------------------------------------------------
    def test_hostname_long(self):
        """
        Calling U.hostname(long=True) or U.hostname(True) should get the
        long hostanme
        """
        self.dbgfunc()
        hn = U.hostname(long=True)
        self.assertTrue('.' in hn,
                        "Expected long hostname but got '%s'" % hn)
        hn = U.hostname(True)
        self.assertTrue('.' in hn,
                        "Expected long hostname but got '%s'" % hn)

    # -------------------------------------------------------------------------
    def test_hostname_short(self):
        """
        Calling U.hostname(long=False) or U.hostname(False) should get
        the short hostname
        """
        self.dbgfunc()
        hn = U.hostname(long=False)
        self.assertFalse('.' in hn,
                         "Expected short hostname but got '%s'" % hn)
        hn = U.hostname(False)
        self.assertFalse('.' in hn,
                         "Expected short hostname but got '%s'" % hn)

    # -------------------------------------------------------------------------
    def test_line_quote(self):
        """
        line_quote is supposed to wrap a string in line-based quote marks
        (three double quotes in a row) on separate lines. Any single or double
        quotes wrapping the incoming string are stripped off in the output.
        """
        self.dbgfunc()
        exp = '\n"""\nabc\n"""'
        act = U.line_quote('abc')
        self.expected(exp, act)

        exp = '\n"""\nabc\n"""'
        act = U.line_quote("'abc'")
        self.expected(exp, act)

        exp = '\n"""\nabc\n"""'
        act = U.line_quote('"abc"')
        self.expected(exp, act)

    # -------------------------------------------------------------------------
    def test_my_name(self):
        """
        Return the name of the calling function.
        """
        self.dbgfunc()
        actual = U.my_name()
        expected = 'test_my_name'
        self.expected(expected, actual)

    # -------------------------------------------------------------------------
    def test_pop0(self):
        """
        Routine pop0() should remove and return the 0th element of a list. If
        the list is empty, it should return None. After pop0() returns, the
        list should be one element shorter.
        """
        self.dbgfunc()
        tl = [1, 2, 3, 4, 5]
        x = copy.copy(tl)
        e = U.pop0(x)
        self.expected(1, e)
        self.expected(tl[1:], x)
        self.expected(len(tl) - 1, len(x))

        x = ['abc']
        self.expected('abc', U.pop0(x))
        self.expected(None, U.pop0(x))
        self.expected(None, U.pop0(x))

    # -------------------------------------------------------------------------
    def test_rgxin(self):
        """
        Routine rgxin(needle, haystack) is analogous to the Python expression
        "needle in haystack" with needle being a regexp.
        """
        self.dbgfunc()
        rgx = "a\(?b\)?c"
        rgx2 = "(dog|fox|over)"
        fstring = "The quick brown fox jumps over the lazy dog"
        tstring1 = "Now we know our abc's"
        tstring2 = "With parens: a(b)c"
        self.assertTrue(U.rgxin(rgx, tstring1),
                        "'%s' should match '%s'" % (rgx, tstring1))
        self.assertTrue(U.rgxin(rgx, tstring2),
                        "'%s' should match '%s'" % (rgx, tstring2))
        self.assertFalse(U.rgxin(rgx, fstring),
                         "'%s' should NOT match '%s'" % (rgx, fstring))
        self.expected('abc', U.rgxin(rgx, tstring1))
        self.expected('a(b)c', U.rgxin(rgx, tstring2))
        self.expected('fox', U.rgxin(rgx2, fstring))

    # -------------------------------------------------------------------------
    def test_rrfile_long(self):
        """
        Test the reverse read file class
        """
        self.dbgfunc()
        tdfile = self.tmpdir(U.my_name())
        clist = [chr(ord('a') + x) for x in range(0, 16)]
        with open(tdfile, 'w') as f:
            for c in clist:
                f.write(c * 64)

        rf = U.RRfile.open(tdfile, 'r')
        zlist = clist
        buf = rf.revread()
        while buf != '':
            ref = zlist[-2:]
            del zlist[-1]
            self.expected(ref[0], buf[0])
            self.expected(ref[-1], buf[-1])
            buf = rf.revread()

        rf.close()

    # -------------------------------------------------------------------------
    def test_rrfile_short(self):
        """
        Test the reverse read file class
        """
        self.dbgfunc()
        tdfile = self.tmpdir(U.my_name())
        clist = [chr(ord('a') + x) for x in range(0, 4)]
        with open(tdfile, 'w') as f:
            for c in clist:
                f.write(c * 16)

        rf = U.RRfile.open(tdfile, 'r')
        zlist = clist
        buf = rf.revread()
        self.expected(64, len(buf))
        for exp in ["aaa", "bbb", "ccc", "ddd"]:
            self.expected_in(exp, buf)

        rf.close()

    # -------------------------------------------------------------------------
    def test_scale(self):
        """
        U.scale("25") should return 25
        """
        # ---------------------------------------------------------------------
        def doit(factor, spec, kb=-1, exponent=1):
            sep = random.choice(['', ' '])
            kw = {'spec': '%d%s%s' % (factor, sep, spec)}
            if 0 < kb:
                kw['kb'] = kb
            if 'i' in spec.lower() or 1024 == kb:
                base = 1024
            else:
                base = 1000
            result = factor * base**exponent
            self.expected(result, U.scale(**kw))

        self.dbgfunc()
        self.expected(1, U.scale())
        self.expected(0, U.scale(''))
        self.expected(5, U.scale('5'))
        self.expected(7, U.scale('7 b'))

        cycle = 1
        for kmgt in ['k', 'm', 'g', 't', 'p', 'e', 'z', 'y']:
            doit(random.randrange(0, 100), kmgt+'b', exponent=cycle)
            doit(random.randrange(0, 100),
                 kmgt.upper()+'b',
                 kb=1024,
                 exponent=cycle)
            doit(random.randrange(0, 100), kmgt+'IB', exponent=cycle)
            cycle += 1

    # -------------------------------------------------------------------------
    def test_touch_newpath_default(self):
        """
        Call touch on a path that does not exist with no amtime tuple

        This test code assumes that file system operations truncate atime and
        mtime rather than rounding them.
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(), new=True)

    # -------------------------------------------------------------------------
    def test_touch_newpath_atime(self):
        """
        Call touch on a path that does not exist with atime, no mtime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(-75, None), new=True)

    # -------------------------------------------------------------------------
    def test_touch_newpath_mtime(self):
        """
        Call touch on a path that does not exist with mtime, no atime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(None, -32), new=True)

    # -------------------------------------------------------------------------
    def test_touch_newpath_both(self):
        """
        Call touch on a path that does not exist with both atime and mtime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(-175, -3423), new=True)

    # -------------------------------------------------------------------------
    def test_touch_oldpath_default(self):
        """
        Call touch on a path that does exist with no amtime tuple
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=())

    # -------------------------------------------------------------------------
    def test_touch_oldpath_atime(self):
        """
        Call touch on a path that does exist with atime, no mtime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(-75, None))

    # -------------------------------------------------------------------------
    def test_touch_oldpath_mtime(self):
        """
        Call touch on a path that does exist with mtime, no atime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(None, -32))

    # -------------------------------------------------------------------------
    def test_touch_oldpath_both(self):
        """
        Call touch on a path that does exist with both atime and mtime
        """
        self.dbgfunc()
        testpath = self.tmpdir(U.my_name())
        self.touch_payload(testpath, offs=(-175, -3423))

    # -------------------------------------------------------------------------
    def touch_payload(self, testpath, offs=(), new=False):
        """
        Testing U.touch.

        *testpath* is the path of the file to be touched.

        *vtup* is a tuple of two integers, (), or None. () and None are
        converted to (0, 0). These values are used as offsets from the current
        time.

        *ctup* is a tuple of two integers, (), or None. If it contains two
        integers, they are applied as offsets from the current time. If it is
        None, None is passed to U.touch(). If it is (), no argument is
        passed to U.touch in that position.

        *new* indicates whether we are touching a new (non-existent) file or a
         file that already exists.
        """
        if new:
            # testing new file -- make sure it does not exist
            U.conditional_rm(testpath)
        else:
            # testing old file -- make sure it DOES exist
            open(testpath, 'a').close()

        # make sure we're early in the current second to avoid boundary issues
        now = self.early_second()

        # run the test
        args = [testpath]
        if offs != ():
            args.append(self.touch_tuple(now, offs))
        U.touch(*args)

        # verify that the file exists
        self.assertPathPresent(testpath)

        # verify that both atime and mtime are close to the correct time
        s = os.stat(testpath)
        (atime, mtime) = self.verify_tuple(now=now, offset=offs)
        self.assertAlmostEquals(atime, s[stat.ST_ATIME],
                                places=0,
                                msg="Expected %d and %d to be close" %
                                (atime, s[stat.ST_ATIME]))
        self.assertAlmostEquals(mtime, s[stat.ST_MTIME],
                                places=0,
                                msg="Expected %d and %d to be close" %
                                (mtime, s[stat.ST_MTIME]))

    # -------------------------------------------------------------------------
    def touch_tuple(self, now, offset):
        """
        Return tuple offset with offset values added to the base time. None
        values are left in place. This tuple is passed as an argument to
        touch().
        """
        if offset is None:
            rval = None
        elif offset[0] is None and offset[1] is not None:
            rval = (None, now + offset[1])
        elif offset[0] is not None and offset[1] is None:
            rval = (now + offset[0], None)
        else:
            rval = (now + offset[0], now + offset[1])
        return rval

    # -------------------------------------------------------------------------
    def verify_tuple(self, now, offset):
        """
        Return (<time>, <time>) for verification
        """
        if offset is None or offset == ():
            rval = (now, now)
        elif offset[0] is None and offset[1] is not None:
            rval = (now, now + offset[1])
        elif offset[0] is not None and offset[1] is None:
            rval = (now + offset[0], now)
        elif offset[0] is not None and offset[1] is not None:
            rval = (now + offset[0], now + offset[1])
        return rval

    # -------------------------------------------------------------------------
    def early_second(self):
        """
        Make sure we're early in the current second to avoid boundary issues
        """
        now = time.time()
        while now - int(now) > 0.99:
            time.sleep(0.01)
            now = time.time()
        return int(now)
