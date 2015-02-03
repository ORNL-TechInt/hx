import inspect
import hx.msg
import os
import pexpect
import pytest
import re
import sys
import hx.testhelp as th
import unittest
import hx.util as U


# -----------------------------------------------------------------------------
class Test_BestPractice(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_0_pep8(self):
        """
        Check code for pep8 conformance
        """
        self.dbgfunc()
        full_result = ""
        for r, d, f in os.walk('.'):
            pylist = [os.path.abspath(os.path.join(r, fn))
                      for fn in f
                      if fn.endswith('.py') and not fn.startswith(".#")]
            inputs = " ".join(pylist)
            if any([r == "./test",
                    ".git" in r,
                    ".attic" in r,
                    "" == inputs]):
                continue
            result = pexpect.run("pep8 %s" % inputs)
            full_result += result.replace(hx.msg.cov_no_data, "")
        self.expected("", full_result)

    # -------------------------------------------------------------------------
    def test_no_duplicates(self):
        """
        Scan all .py files for duplicate function names
        """
        self.dbgfunc()
        dupl = {}
        for r, d, f in os.walk('.'):
            for fname in f:
                path = os.path.join(r, fname)
                if "CrawlDBI" in path:
                    continue
                if path.endswith(".py") and not fname.startswith(".#"):
                    result = check_for_duplicates(path)
                    if result != '':
                        dupl[path] = result
        if dupl != {}:
            rpt = ''
            for key in dupl:
                rpt += "Duplicates in %s:" % key + dupl[key] + "\n"
            self.fail(rpt)


# -----------------------------------------------------------------------------
def check_for_duplicates(path):
    """
    Scan *path* for duplicate function names
    """
    rx_def = rgx_def(0)
    rx_cls = rgx_class(0)

    flist = {}
    rval = ''
    cur_class = ''
    lno = 1
    with open(path, 'r') as f:
        for l in f.readlines():
            q = rx_cls.match(l)
            if q:
                cur_class = q.groups()[0] + '.'

            q = rx_def.match(l)
            if q:
                cur_def = q.groups()[0]
                # flist.append(cur_class + cur_def + " (%d)" % lno)
                k = cur_class + cur_def
                try:
                    flist[k].append(lno)
                except KeyError:
                    flist[k] = [lno]

            lno += 1

    for k in [x for x in flist if 1 < len(flist[x])]:
        rval += "\n   %s:%d" % (k, flist[k][1])

    return rval


# -----------------------------------------------------------------------------
@U.memoize
def rgx_def(obarg):
    """
    Return a compiled regex for finding function definitions
    """
    return re.compile("^\s*def\s+(\w+)\s*\(")


# -----------------------------------------------------------------------------
@U.memoize
def rgx_class(obarg):
    """
    Return a compiled regex for finding class definitions
    """
    return re.compile("^\s*class\s+(\w+)\s*\(")


# -----------------------------------------------------------------------------
def test_nodoc():
    """
    Report routines missing a doc string
    """
    pytest.dbgfunc()

    modname = 'hx'

    # get our bearings -- where is the module?
    mod_dir = U.dirname(sys.modules[modname].__file__)

    excludes = ['setup.py', '__init__.py']

    # up a level from there, do we have a '.git' directory? That is, are we in
    # a git repository? If so, we want to talk the whole repo for .py files
    mod_par = U.dirname(mod_dir)
    if not os.path.isdir(U.pathjoin(mod_par, ".git")):
        # otherwise, we just work from hpssic down
        wroot = mod_dir
    else:
        wroot = mod_par

    # collect all the .py files in pylist
    pylist = []
    for r, dlist, flist in os.walk(wroot):
        if '.git' in dlist:
            dlist.remove('.git')
        pylist.extend([U.pathjoin(r, x)
                       for x in flist
                       if x.endswith('.py') and x not in excludes])

    # make a list of the modules implied in pylist in mdict. Each module name
    # is a key. The associated value is False until the module is checked.
    mlist = [modname]
    for path in pylist:
        # Throw away the hpssic parent string, '.py' at the end, and split on
        # '/' to get a list of the module components
        mp = path.replace(mod_par + '/', '').replace('.py', '').split('/')

        if 1 < len(mp):
            fromlist = [modname]
        else:
            fromlist = []
        mname = '.'.join(mp)
        if mname.startswith(modname):
            mlist.append(mname)
        if mname not in sys.modules and mname.startswith(modname):
            try:
                __import__(mname, fromlist=fromlist)

            except ImportError:
                pytest.fail('Failure trying to import %s' % mname)

    result = ''
    for m in mlist:
        result += nodoc_check(sys.modules[m], pylist, 0, 't')

    # result = nodoc_check(hpssic, 0, 't')
    if result != '':
        result = "\nFunctions with no docstring:" + result
        pytest.fail(result)


# -----------------------------------------------------------------------------
def nodoc_check(mod, pylist, depth, why):
    """
    Walk the tree of modules and classes looking for routines with no doc
    string and report them
    """
    # -------------------------------------------------------------------------
    def filepath_reject(obj, pylist):
        """
        Reject the object based on its filepath
        """
        if hasattr(obj, '__file__'):
            fpath = obj.__file__.replace('.pyc', '.py')
            rval = fpath not in pylist
        elif hasattr(obj, '__func__'):
            fpath = obj.__func__.func_code.co_filename.replace('.pyc', '.py')
            rval = fpath not in pylist
        else:
            rval = False

        return rval

    # -------------------------------------------------------------------------
    def name_accept(name, already):
        """
        Whether to accept the object based on its name
        """
        rval = True
        if all([name in dir(unittest.TestCase),
                name not in dir(th.HelpedTestCase)]):
            rval = False
        elif name in already:
            rval = False
        elif all([name.startswith('__'),
                  name != '__init__']):
            rval = False

        return rval

    # -------------------------------------------------------------------------
    global count
    try:
        already = nodoc_check._already
    except AttributeError:
        count = 0
        nodoc_check._already = ['AssertionError',
                                'base64',
                                'bdb',
                                'contextlib',
                                'datetime',
                                'decimal',
                                'difflib',
                                'dis',
                                'email',
                                'errno',
                                'fcntl',
                                'getopt',
                                'getpass',
                                'glob',
                                'heapq',
                                'inspect',
                                'InterpolationMissingOptionError',
                                'linecache',
                                'logging',
                                'MySQLdb',
                                'NoOptionError',
                                'NoSectionError',
                                'optparse',
                                'os',
                                'pdb',
                                'pexpect',
                                'pickle',
                                'pprint',
                                'pwd',
                                'pytest',
                                're',
                                'shlex',
                                'shutil',
                                'smtplib',
                                'socket',
                                'sqlite3',
                                'ssl',
                                'stat',
                                'StringIO',
                                'sys',
                                'tempfile',
                                'text',
                                'timedelta',
                                'times',
                                'tokenize',
                                'traceback',
                                'unittest',
                                'urllib',
                                'warnings',
                                'weakref',
                                ]
        already = nodoc_check._already

    rval = ''

    if any([mod.__name__ in already,
            U.safeattr(mod, '__module__') == '__builtin__',
            filepath_reject(mod, pylist),
            ]):
        return rval

    # print("nodoc_check(%s = %s)" % (mod.__name__, str(mod)))
    for name, item in inspect.getmembers(mod, inspect.isroutine):
        if all([not inspect.isbuiltin(item),
                not filepath_reject(item, pylist),
                name_accept(item.__name__, already)
                ]):
            if item.__doc__ is None:
                try:
                    filename = U.basename(mod.__file__)
                except AttributeError:
                    tmod = sys.modules[mod.__module__]
                    filename = U.basename(tmod.__file__)
                rval += "\n%3d. %s(%s): %s" % (count,
                                               filename,
                                               why,
                                               item.__name__)
                try:
                    count += 1
                except NameError:
                    count = 1
            already.append(":".join([mod.__name__, name]))

    for name, item in inspect.getmembers(mod, inspect.isclass):
        if all([item.__name__ not in already,
                depth < 5]):
            rval += nodoc_check(item, pylist, depth+1, 'c')
            already.append(item.__name__)

    for name, item in inspect.getmembers(mod, inspect.ismodule):
        if all([not inspect.isbuiltin(item),
                item.__name__ not in already,
                not name.startswith('@'),
                not name.startswith('_'),
                depth < 5]):
            rval += nodoc_check(item, pylist, depth+1, 'm')
            already.append(item.__name__)

    return rval
