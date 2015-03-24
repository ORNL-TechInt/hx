"""
Extensions to python's standard unittest module

 - Add command line options in testhelp.main().

 - Test setup/teardown support for creating and removing a directory to hold
   test data.

 - Test logging (LoggingTestSuite).

 - Test selection from the command line.

 - HelpedTestCase:
    > self.expected() compares an expected and actual value and reports diffs

"""
import cfg
import dbi
import glob
import optparse
import os
import pdb
import pytest
import re
import shutil
import sys
import time
import unittest
import util as U


# -----------------------------------------------------------------------------
def rm_cov_warn(string):
    """
    Return the input string with the coverage warning ('Coverage.py warning: no
    data was collected') removed if present.
    """
    rval = string
    covwarn = "Coverage.py.warning:.No.data.was.collected.\r?\n?"
    if re.findall(covwarn, string):
        rval = re.sub(covwarn, "", string)
    return rval


# -----------------------------------------------------------------------------
class HelpedTestCase(unittest.TestCase):
    """
    This class adds some goodies to the standard unittest.TestCase
    """
    # -------------------------------------------------------------------------
    def assertPathPresent(self, pathname, umsg=''):
        """
        Verify that a path exists or throw an assertion error
        """
        msg = (umsg or "Expected file '%s' to exist to but it does not" %
               pathname)
        self.assertTrue(os.path.exists(pathname), msg)

    # -------------------------------------------------------------------------
    def assertPathNotPresent(self, pathname, umsg=''):
        """
        Verify that a path does not exist or throw an assertion error
        """
        msg = (umsg or "Expected file '%s' to not exist to but it does" %
               pathname)
        self.assertFalse(os.path.exists(pathname), msg)

    # -------------------------------------------------------------------------
    def assertRaisesRegex(self, exception, message, func, *args, **kwargs):
        """
        A more precise version of assertRaises so we can validate the content
        of the exception thrown.
        """
        try:
            func(*args, **kwargs)
        except exception as e:
            if type(message) == str:
                self.assertTrue(re.findall(message, str(e)),
                                "\nExpected '%s', \n     got '%s'" %
                                (message, str(e)))
            elif type(message) == list:
                self.assertTrue(any(re.findall(t, str(e)) for t in message),
                                "Expected one of '%s', got '%s'" % (message,
                                                                    str(e)))
            else:
                self.fail("message must be a string or list")
        except Exception as e:
            self.fail('Unexpected exception thrown: %s %s' % (type(e), str(e)))
        else:
            self.fail('Expected exception %s not thrown' % exception)

    # -------------------------------------------------------------------------
    def assertRaisesMsg(self, exception, message, func, *args, **kwargs):
        """
        A more precise version of assertRaises so we can validate the content
        of the exception thrown.
        """
        try:
            func(*args, **kwargs)
        except exception as e:
            if type(message) == str:
                self.assertTrue(message in str(e),
                                "\nExpected '%s', \n     got '%s'" %
                                (message, str(e)))
            elif type(message) == list:
                self.assertTrue(any(t in str(e) for t in message),
                                "Expected one of '%s', got '%s'" % (message,
                                                                    str(e)))
            else:
                self.fail("message must be a string or list")
        except Exception as e:
            self.fail('Unexpected exception thrown: %s %s' % (type(e), str(e)))
        else:
            self.fail('Expected exception %s not thrown' % exception)

    # -------------------------------------------------------------------------
    def expected(self, expval, actual):
        """
        Compare the expected value (expval) and the actual value (actual). If
        there are differences, report them.
        """
        msg = "\nExpected: "
        if type(expval) == int:
            msg += "%d"
        elif type(expval) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        msg += "\n  Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        self.assertEqual(expval, actual, msg % (expval, actual))

    # -------------------------------------------------------------------------
    def expected_in(self, exprgx, actual):
        """
        If the expected regex (exprgx) does not appear in the actual value
        (actual), report the assertion failure.
        """
        msg = "\nExpected_in: "
        if type(exprgx) == int:
            msg += "%d"
            exprgx = "%d" % exprgx
        elif type(exprgx) == float:
            msg += "%g"
            exprgx = "%g" % exprgx
        else:
            msg += "'%s'"

        msg += "\n     Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        if type(actual) == list:
            if type(exprgx) == str:
                self.assertTrue(any([U.rgxin(exprgx, x) for x in actual]),
                                msg % (exprgx, actual))
            else:
                self.assertTrue(exprgx in actual, msg % (exprgx, actual))
        elif type(exprgx) == tuple:
            self.assertTrue(exprgx in actual, msg % (exprgx, actual))
        else:
            self.assertTrue(U.rgxin(exprgx, actual), msg % (exprgx, actual))

    # -------------------------------------------------------------------------
    def unexpected(self, expval, actual):
        """
        Compare the expected value (expval) and the actual value (actual). If
        there are no differences, fail.
        """
        msg = "\nUnexpected: "
        if type(expval) == int:
            msg += "%d"
        elif type(expval) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        msg += "\n    Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        self.assertNotEqual(expval, actual, msg % (expval, actual))

    # -------------------------------------------------------------------------
    def unexpected_in(self, exprgx, actual):
        """
        If the unexpected regex (exprgx) appears in the actual value (actual),
        report the assertion failure.
        """
        msg = "\nUnexpected_in: "
        if type(exprgx) == int:
            msg += "%d"
            exprgx = "%d" % exprgx
        elif type(exprgx) == float:
            msg += "%g"
            exprgx = "%g" % exprgx
        else:
            msg += "'%s'"

        msg += "\n     Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        if type(actual) == list:
            if type(exprgx) == str:
                self.assertFalse(all([U.rgxin(exprgx, x) for x in actual]),
                                 msg % (exprgx, actual))
            else:
                self.assertFalse(exprgx in actual, msg % (exprgx, actual))
        else:
            self.assertFalse(U.rgxin(exprgx, actual),
                             msg % (exprgx, actual))

    # -------------------------------------------------------------------------
    def noop(self):
        """
        Do nothing
        """
        pass

    # ------------------------------------------------------------------------
    def setUp(self):
        """
        Set self.dbgfunc to either pdb.set_trace or a no-op, depending on the
        value of the --dbg option from the command line
        """
        dbgopt = pytest.config.getoption("dbg")
        stn = '.' + self._testMethodName
        if stn in dbgopt or '.all' in dbgopt:
            pdb.set_trace()

        fullname = "%s.%s" % (self.__class__, self._testMethodName)
        if any([x in fullname for x in dbgopt] +
               ["all" in dbgopt]):
            self.dbgfunc = pdb.set_trace
        else:
            self.dbgfunc = lambda: None

    # -------------------------------------------------------------------------
    @pytest.fixture(autouse=True)
    def tmpdir_setup(self, tmpdir):
        """
        Copy the pytest tmpdir for this test into an attribute on the object
        """
        self.pytest_tmpdir = str(tmpdir)

    # ------------------------------------------------------------------------
    def tmpdir(self, base=''):
        """
        Return the pytest tmpdir for this test with an optional basename
        appended
        """
        if base:
            rval = U.pathjoin(self.pytest_tmpdir, base)
        else:
            rval = U.pathjoin(self.pytest_tmpdir)
        return rval

    # ------------------------------------------------------------------------
    def logpath(self, basename=''):
        """
        Return the logpath for this test. The test can specify the basename but
        does not have to.
        """
        if basename == '':
            basename = "test_default_hpss_crawl.log"
        rval = self.tmpdir(basename)
        return rval

    # ------------------------------------------------------------------------
    def write_cfg_file(self, fname, cfgdict, includee=False):
        """
        !@! too hpssic specific, duplicates crawl_write
        Write a config file for testing. Put the 'crawler' section first.
        Complain if the 'crawler' section is not present.
        """
        if isinstance(cfgdict, dict):
            cfobj = cfg.config.dictor(cfgdict)
        elif isinstance(cfgdict, cfg.config):
            cfobj = cfgdict
        else:
            raise StandardError("cfgdict has invalid type %s" % type(cfgdict))

        if 'crawler' not in cfobj.sections() and not includee:
            raise StandardError("section '%s' missing from test config file" %
                                "crawler")

        with open(fname, 'w') as f:
            cfobj.write(f)
