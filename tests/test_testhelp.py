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
import hx.cfg
import pdb
import sys
import StringIO
import hx.testhelp
import unittest


# -----------------------------------------------------------------------------
class TesthelpTest(hx.testhelp.HelpedTestCase):
    """
    Tests for testhelp code.
    """
    # -------------------------------------------------------------------------
    def test_expected_vs_got(self):
        """
        testhelpTest.test_expected_vs_got
        Test expected_vs_got(). If expected and got match, the output should be
        empty. If they don't match, this should be reported. Again, we have to
        redirect stdout.
        """
        self.dbgfunc()
        self.redirected_evg('', '', '')
        self.redirected_evg('one', 'two',
                            "EXPECTED: 'one'\n" +
                            "GOT:      'two'\n")

    # -------------------------------------------------------------------------
    def test_HelpedTestCase(self):
        """
        Verify that a HelpedTestCase object has the expected attributes
        """
        self.dbgfunc()
        q = hx.testhelp.HelpedTestCase(methodName='noop')
        for attr in ['expected', 'expected_in', 'write_cfg_file']:
            self.assertTrue(hasattr(q, attr),
                            "Expected %s to have attr %s" % (q, attr))

    # -------------------------------------------------------------------------
    def redirected_evg(self, exp, got, expected):
        """
        Redirect stdout, run expected_vs_got() and return the result
        """
        s = StringIO.StringIO()
        save_stdout = sys.stdout
        sys.stdout = s
        try:
            self.expected(exp, got)
        except AssertionError:
            pass
        r = s.getvalue()
        s.close()
        sys.stdout = save_stdout

        try:
            assert(r.startswith(expected))
        except AssertionError:
            print "expected: '''\n%s'''" % expected
            print "got:      '''\n%s'''" % r
