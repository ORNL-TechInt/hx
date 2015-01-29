import contextlib
import copy
import logging
import logging.handlers as logh
# import messages as MSG
import os
import pdb
import random
import re
import shutil
import socket
import string
import sys
import time
import traceback as tb


# -----------------------------------------------------------------------------
@contextlib.contextmanager
def tmpenv(name, val):
    """
    This is the simpler way of defining a context manager (as opposed to
    writing a whole class with __init__(), __enter__() and __exit__()
    routines). We can use this one like so:

        assert(os.getenv('FOO') == 'xyz'
        with tmpenv('FOO', 'bar'):
            assert(os.getenv('FOO') == 'bar')
        assert(os.getenv('FOO') == 'xyz'

    A variable can be removed from the environment by setting it to None. For
    example,

        assert(os.getenv('FOO') == 'xyz'
        with tmpenv('FOO', None):
            assert(os.getenv('FOO') is None)
        assert(os.getenv('FOO') == 'xyz'
    """
    prev = os.getenv(name)
    if val is not None:
        os.environ[name] = val
    elif prev is not None:
        del os.environ[name]

    yield

    if prev is not None:
        os.environ[name] = prev
    elif os.getenv(name) is not None:
        del os.environ[name]


# -----------------------------------------------------------------------------
class Chdir(object):
    """
    This class allows for doing the following:

        with Chdir('/some/other/directory'):
            assert(in '/some/other/directory')
            do_stuff()
        assert(back at our starting point)

    No matter what happens in do_stuff(), we're guaranteed that at the assert,
    we'll be back in the directory we started from.
    """

    # ------------------------------------------------------------------------
    def __init__(self, target):
        """
        This is called at instantiattion. Here we just initialize.
        """
        self.start = os.getcwd()
        self.target = target

    # ------------------------------------------------------------------------
    def __enter__(self):
        """
        This is called as control enters the with block. We jump to the target
        directory.
        """
        os.chdir(self.target)
        return self.target

    # ------------------------------------------------------------------------
    def __exit__(self, type, value, traceback):
        """
        This is called as control leaves the with block. We jump back to our
        starting directory.
        """
        os.chdir(self.start)


# -----------------------------------------------------------------------------
class ArchiveLogfileHandler(logh.RotatingFileHandler, object):
    """
    This an augmented RotatingFileHandler. At initialization, it accepts an
    archive directory, which is not passed along to the super. At rollover
    time, it also copies the rolled over file to the archive directory with a
    name based on the first and last timestamp in the file.
    """
    # ------------------------------------------------------------------------
    def __init__(self, filename, **kwargs):
        """
        Handle archdir and remove it. Let super() deal with the rest of them.
        """
        if ('archdir' in kwargs):
            if kwargs['archdir'] != '':
                self.archdir = kwargs['archdir']
            del kwargs['archdir']
        super(ArchiveLogfileHandler, self).__init__(filename, **kwargs)

    # ------------------------------------------------------------------------
    def doRollover(self):
        """
        After the normal rollover when a log file fills, we want to copy the
        newly rolled over file (my_log.1) to the archive directory. If an
        archive directory has not been set, we behave just like our parent and
        don't archive the log file at all.

        The copied file will be named my_log.<start-date>-<end-date>
        """
        super(ArchiveLogfileHandler, self).doRollover()

        try:
            archdir = self.archdir
        except AttributeError:
            return

        path1 = self.baseFilename + ".1"
        target = "%s/%s.%s-%s" % (archdir,
                                  os.path.basename(self.baseFilename),
                                  date_start(path1),
                                  date_end(path1))
        if not os.path.isdir(archdir):
            os.makedirs(archdir)
        shutil.copy2(path1, target)


# -----------------------------------------------------------------------------
class RRfile(object):
    """
    This is a thin wrapper around the file type that adds a reverse read method
    so a file can be read backwards.
    """
    # ------------------------------------------------------------------------
    def __init__(self, filename, mode):
        """
        Initialize the file -- open it and position for the first read.
          chunk: how many bytes to read at a time
          bof: beginning of file, are we done yet? -- analogous to eof
          f: file handle
        """
        self.chunk = 128
        self.bof = False
        self.f = open(filename, mode)
        self.f.seek(0, os.SEEK_END)
        size = self.f.tell()
        if self.chunk < size:
            self.f.seek(-self.chunk, os.SEEK_CUR)
        else:
            self.f.seek(0, os.SEEK_SET)

    # ------------------------------------------------------------------------
    @classmethod
    def open(cls, filename, mode):
        """
        This is how we get a new one of these.
        """
        new = RRfile(filename, mode)
        return new

    # ------------------------------------------------------------------------
    def close(self):
        """
        Close the file this object contains.
        """
        self.f.close()

    # ------------------------------------------------------------------------
    def revread(self):
        """
        Read backwards on the file in chunks defined by self.chunk. We start
        out at the place where we should read next. After reading, we seek back
        1.5 * self.chunk so our next read will overlap the last one. This is so
        that date expressions that span a read boundary will be kept together
        on the next read.
        """
        if self.bof:
            return ''

        if self.f.tell() == 0:
            self.bof = True

        rval = self.f.read(self.chunk)
        try:
            self.f.seek(-self.chunk, os.SEEK_CUR)
            self.f.seek(-self.chunk/2, os.SEEK_CUR)
        except IOError:
            self.f.seek(0, os.SEEK_SET)

        return rval


# -----------------------------------------------------------------------------
def abspath(relpath):
    """
    Convenience wrapper for os.path.abspath()
    """
    return os.path.abspath(relpath)


# -----------------------------------------------------------------------------
def basename(path):
    """
    Convenience wrapper for os.path.basename()
    """
    return os.path.basename(path)


# -----------------------------------------------------------------------------
def default_logpath():
    """
    Return the ultimate default log path
    """
    if os.getuid() == 0:
        rval = "/var/log/hpssic.log"
    else:
        rval = "/tmp/hpssic.log"
    return rval


# -----------------------------------------------------------------------------
def default_plugins():
    """
    Return the default list of currently defined plugins. This should be the
    only place in the system where they are listed, so when the list changes,
    only this routine need be updated.
    """
    return "cv,mpra,rpt,tcc"


# -----------------------------------------------------------------------------
def dirname(path, layers=1):
    """
    Convenience wrapper for os.path.dirname(). With optional *layers* argument,
    remove that many layers. If the result is empty, return '.'
    """
    rval = path
    for n in range(layers):
        rval = os.path.dirname(rval)
    return rval or '.'


# -----------------------------------------------------------------------------
def expand(path):
    """
    Expand ~user and environment variables in a string
    """
    def parse(var):
        z = re.search("([^$]*)(\$({([^}]*)}|\w*))(.*)", var)
        if z:
            return(z.groups()[0],
                   z.groups()[3] or z.groups()[2],
                   z.groups()[-1])
        else:
            return(None, None, None)

    rval = os.path.expanduser(os.path.expandvars(path))
    while '$' in rval:
        pre, var, post = parse(rval)
        if ':' in var:
            vname, vdef = var.split(":")
            vval = os.getenv(vname, vdef[1:])
        else:
            vval = os.getenv(var, "")
        rval = pre + os.path.expanduser(vval) + post
    return rval


# -----------------------------------------------------------------------------
def pathjoin(a, *p):
    """
    Convenience wrapper for os.path.join()
    """
    return os.path.join(a, *p)


# -----------------------------------------------------------------------------
def conditional_rm(filepath, tree=False):
    """
    We want to delete filepath but we don't want to generate an error if it
    doesn't exist. Return the existence value of filepath at call time.

    If tree is true, the caller is saying that he knows filepath is a directory
    that may not be empty and he wants to delete it regardless. If the caller
    does not specify tree and the target is a non-empty directory, this call
    will fail.
    """
    rv = False
    if os.path.islink(filepath):
        rv = True
        os.unlink(filepath)
    elif os.path.isdir(filepath):
        rv = True
        if tree:
            shutil.rmtree(filepath)
        else:
            os.rmdir(filepath)
    elif os.path.exists(filepath):
        rv = True
        os.unlink(filepath)
    return rv


# -----------------------------------------------------------------------------
def contents(filename, string=True):
    """
    Return the contents of the file. If string is True, we return a string,
    otherwise a list.
    """
    f = open(filename, 'r')
    if string:
        rval = f.read()
    else:
        rval = f.readlines()
    f.close()
    return rval


# ------------------------------------------------------------------------------
def csv_list(value, delimiter=","):
    """
    Split a string on a delimiter and return the resulting list, stripping away
    whitespace.
    """
    if value.strip() == '':
        rval = []
    elif delimiter not in value:
        rval = [value.strip()]
    else:
        rval = [x.strip() for x in value.split(delimiter)]
    return rval


# -----------------------------------------------------------------------------
def day_offset(days):
    """
    Return the epoch of 0:00am *days* days from today. If *days* is 0, we get
    0:00am today. If *days* is -1, it's 0:00am yesterday. And *days* == 1 means
    0:00am tomorrow.
    """
    rval = daybase(time.time()) + days * 3600 * 24
    return rval


# -----------------------------------------------------------------------------
def daybase(epoch):
    """
    Given an epoch time, *epoch*, return the beginning of the day containing
    the input.
    """
    tm = time.localtime(epoch)
    return time.mktime([tm.tm_year, tm.tm_mon, tm.tm_mday,
                        0, 0, 0,
                        tm.tm_wday, tm.tm_yday, tm.tm_isdst])


# -----------------------------------------------------------------------------
def dispatch(modname, prefix, args):
    """
    Look in module *modname* for routine *prefix*_*args*[1]. Call it with
    *args*[2:].
    """
    mod = sys.modules[modname]
    if len(args) < 2:
        dispatch_help(mod, prefix)
    elif len(args) < 3 and args[1] == 'help':
        dispatch_help(mod, prefix)
    elif args[1] == 'help':
        dispatch_help(mod, prefix, args[2])
    else:
        fname = "_".join([prefix, args[1]])
        func = getattr(mod, fname)
        func(args[2:])


# -----------------------------------------------------------------------------
def dispatch_help(mod, prefix, cmd=None):
    """
    Display help as appropriate. If cmd is None, report a one liner for each
    available function. If cmd is not None, look up its doc string and report
    it.
    """
    if cmd is not None:
        func = getattr(mod, "_".join([prefix, cmd]))
        print func.__doc__
    else:
        print("")
        for fname in [x for x in dir(mod) if x.startswith(prefix)]:
            func = getattr(mod, fname)
            try:
                hstr = func.__doc__.split("\n")[0]
            except AttributeError:
                raise HpssicError(
                    "Function '%s' seems to be missing a docstring" % fname)
            print "    " + hstr
        print("")


# -----------------------------------------------------------------------------
def env_update(cfg):
    """
    Update the environment based on the contents of the 'env' section of the
    config object.
    """
    if not cfg.has_section('env'):
        return

    for var in cfg.options('env'):
        uvar = var.upper()
        value = re.sub("\n\s*", "", cfg.get('env', var))
        pre = os.getenv(uvar)
        if pre is not None and value.startswith('+'):
            os.environ[uvar] = ':'.join(os.environ[uvar].split(':') +
                                        value[1:].split(':'))
        elif value.startswith('+'):
            os.environ[uvar] = value[1:]
        else:
            os.environ[uvar] = value


# -----------------------------------------------------------------------------
def epoch(ymdhms):
    """
    Given a string containing a date and/or time, attempt to parse it into an
    epoch time.
    """
    fmts = ["%Y.%m%d %H:%M:%S",
            "%Y.%m%d.%H.%M.%S",
            "%Y.%m%d %H:%M",
            "%Y.%m%d.%H.%M",
            "%Y.%m%d %H",
            "%Y.%m%d.%H",
            "%Y.%m%d",
            ]
    fp = copy.copy(fmts)
    rval = None
    while rval is None:
        try:
            rval = time.mktime(time.strptime(ymdhms, fp.pop(0)))
        except ValueError:
            rval = None
        except IndexError:
            if ymdhms.isdigit():
                rval = int(ymdhms)
            else:
                err = ("The date '%s' does not match any of the formats: %s" %
                       (ymdhms, fmts))
                raise HpssicError(err)

    return rval


# -----------------------------------------------------------------------------
def filename():
    """
    Return the name of the file where the currently running code resides.
    """
    return sys._getframe(1).f_code.co_filename


# -----------------------------------------------------------------------------
def foldsort(seq):
    """
    Return seq in sorted order without regard to case (i.e., 'folded')
    """
    return sorted(seq, cmp=lambda x, y: cmp(x.lower(), y.lower()))


# -----------------------------------------------------------------------------
def hostname(long=False):
    """
    Return the name of the current host.
    """
    if long:
        rval = socket.gethostname()
    else:
        rval = socket.gethostname().split('.')[0]
    return rval


# -----------------------------------------------------------------------------
def git_repo(path):
    """
    If path is inside a git repo (including the root), return the root of the
    git repo. Otherwise, return ''
    """
    if not os.path.isabs(path):
        path = abspath(path)
    dotgit = pathjoin(path, ".git")
    while not os.path.exists(dotgit) and path != "/":
        path = dirname(path)
        dotgit = pathjoin(path, ".git")

    return path.rstrip('/')


# -----------------------------------------------------------------------------
def line_quote(value):
    """
    Wrap a set of lines with line-oriented quotes (three double quotes in a
    row).
    """
    if type(value) == str and value.startswith("'"):
        rv = value.strip("'")
    elif type(value) == str and value.startswith('"'):
        rv = value.strip('"')
    else:
        rv = value
    return '\n"""\n%s\n"""' % str(rv)


# -----------------------------------------------------------------------------
def lineno():
    """
    Return the line number of the file where the currently running code
    resides.
    """
    return sys._getframe(1).f_lineno


# -----------------------------------------------------------------------------
def lsp_parse(lspout):
    """
    We assume *lspout* comes from an hsi 'ls -P' command and parse it as such,
    returning file type ('f' or 'd'), file name, cartridge (if available), and
    cos (if available). Directories don't have cartridge or cos values.
    """
    for line in lspout.split("\r\n"):
        result = re.findall("(FILE|DIRECTORY)", line)
        if [] != result:
            break

    if [] == result:
        raise HpssicError(MSG.lsp_output_not_found)

    x = line.split("\t")
    itype = pop0(x)        # 'FILE' or 'DIRECTORY'
    if itype == 'FILE':
        itype = 'f'
    elif itype == 'DIRECTORY':
        itype = 'd'
    else:
        raise HpssicError(MSG.lsp_invalid_file_type)
    iname = pop0(x)        # name of file or dir
    pop0(x)
    pop0(x)
    pop0(x)
    cart = pop0(x)         # name of cart
    if cart is not None:
        cart = cart.strip()
    cos = pop0(x)          # name of cos
    if cos is not None:
        cos = cos.strip()
    else:
        cos = ''
    return(itype, iname, cart, cos)


# -----------------------------------------------------------------------------
def map_size_unit(spec, kb=1000):
    """
    b  -> 1
    kb -> 1000**1       kib -> 1024
    mb -> 1000**2       mib -> 1024**2
    gb -> 1000**3       gib -> 1024**3
    tb -> 1000**4       tib -> 1024**4
    pb -> 1000**5       pib -> 1024**5
    eb -> 1000**6       eib -> 1024**6
    zb -> 1000**7       zib -> 1024**7
    yb -> 1000**8       yib -> 1024**8

    We use *kb* as the base so the caller can force 'kb' to mean 1024.
    """
    sl = spec.lower()
    if spec:
        k = sl[0] + sl[-1]
        done = False
        while not done:
            try:
                exponent = map_size_unit._expmap[k]
                done = True
            except AttributeError:
                map_size_unit._expmap = {'kb': 1,
                                         'mb': 2,
                                         'gb': 3,
                                         'tb': 4,
                                         'pb': 5,
                                         'eb': 6,
                                         'zb': 7,
                                         'yb': 8}
                done = False
            except KeyError:
                exponent = 0
                done = True

    else:
        exponent = 0

    if 'i' in sl:
        rval = 1024 ** exponent
    else:
        rval = kb ** exponent

    return rval


# -----------------------------------------------------------------------------
def memoize(f):
    """
    This makes available the @util.memoize function decorator. Functions
    decorated with memoize will actually be called the first time the code
    references them. Whatever is returned by that first call will be cached and
    returned on subsequent calls with the same arguments.
    """
    cache = {}

    # -------------------------------------------------------------------------
    def helper(x=''):
        """
        Handle first calls based on the argument. Whenever a unique argument
        come in, we call the function for real. If we've seen the argument
        before, we return what we've cached.
        """
        try:
            return cache[x]
        except KeyError:
            cache[x] = f(x)
            return cache[x]
    return helper


# -------------------------------------------------------------------------
@memoize
def month_dict(arg=None):
    """
    Construct a dictionary mapping month names to month numbers
    """
    mdict = {}
    dt = [0, 0, 0, 0, 0, 0, 0, 0, 0]
    for month in range(1, 13):
        dt[1] = month
        mname = time.strftime("%b", dt)
        mdict[mname] = month
    return mdict


# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name


# -----------------------------------------------------------------------------
def pop0(list):
    """
    Pop and return the 0th element of a list. If the list is empty, return
    None.
    """
    try:
        rval = list.pop(0)
    except IndexError:
        rval = None
    return rval


# -----------------------------------------------------------------------------
def realpath(fakepath):
    """
    Convenience wrapper for os.path.realpath()
    """
    return os.path.realpath(fakepath)


# -----------------------------------------------------------------------------
def rgxin(needle, haystack):
    """
    If the regexp *needle* matches haystack, return the value of the first
    match.

    NOTE: The return value has changed from True/False to indicate whether a
    match was found to the value of the first match found. Normally, this
    should not change the behavior of the routine but there is one case that
    will now indicate False that used to indicate True. That is, if the rgx
    matches the empty string, the empty string will be returned. Before this
    update, that situation would have returned True. Now the empty string
    evaluated in a boolean context will look like False.

    One way to finesse this would be the following usage:

       if util.rgxin(...) is not None:
          ...
    """
    hits = re.findall(needle, haystack)
    rval = pop0(hits)
    return rval


# -----------------------------------------------------------------------------
def safeattr(obj, attr):
    """
    Return the value of *attr* on *obj* or None
    """
    if hasattr(obj, attr):
        return getattr(obj, attr)
    else:
        return None


# -----------------------------------------------------------------------------
def scale(spec='1', kb=1000):
    """
    Scale an expression like '20kb', '1MB', '5 Gib', etc., returning the
    corresponding numeric value. *kb* is used as the base so the caller can
    force 'kb' to mean 1024 rather than 1000.
    """
    hits = re.findall("(\d+)\s*(\w*)", spec)
    if hits:
        (mag, unit) = hits[0]
        factor = map_size_unit(unit, kb=kb)
        rval = int(mag) * factor
    else:
        rval = 0
    return rval


# -----------------------------------------------------------------------------
def squash(string):
    """
    Squeeze all occurrences of whitespace down to a single space and strip any
    whitespace off the beginning and end.
    """
    rval = re.sub("\s\s+", " ", string)
    return rval.strip()


# -----------------------------------------------------------------------------
def touch(pathname, amtime=None):
    """
    If *pathname* does not exist, create it. Update the atime and mtime to the
    current time or, optionally, the contents of the tuple *amtime*.
    """
    if not os.path.exists(pathname):
        open(pathname, 'a').close()

    ztime = amtime_tuple(base=amtime)
    os.utime(pathname, ztime)


# -----------------------------------------------------------------------------
def amtime_tuple(base=None):
    """
    *base* can be None, (), (None, <int>), (<int>, None), or (<int>,<int>)

    This is called from util.touch() to replace (None, <time>), or (<time>,
    None) with a tuple suitable to be passed to os.utime(). os.utime() can
    handle None or (<time>, <time>) but not a tuple containing Nones, which
    seems reasonable if we only want to specify one time or the other.
    """
    now = int(time.time())

    if base is None:
        rval = (now, now)
    elif base[0] is None and base[1] is not None:
        rval = (now, base[1])
    elif base[0] is not None and base[1] is None:
        rval = (base[0], now)
    else:
        rval = (base[0], base[1])

    return rval


# -----------------------------------------------------------------------------
def date_parse(data, idx):
    """
    Compile and cache the regexp for parsing dates from log files.
    """
    try:
        rgx = date_parse._tsrx
    except AttributeError:
        date_parse._fail = "yyyy.mmmm.hhmm"
        rgx_s = r"^(\d{4})\.(\d{4})\s+\d{2}:\d{2}:\d{2}"
        date_parse._tsrx = re.compile(rgx_s, re.MULTILINE)
        rgx = date_parse._tsrx

    q = rgx.findall(data)
    if q is None or q == []:
        rval = date_parse._fail
    else:
        rval = "%s.%s" % (q[idx][0], q[idx][1])
    return rval


# -----------------------------------------------------------------------------
def date_end(filename):
    """
    Read filename and return the last timestamp.
    """
    rval = date_parse('', 0)
    f = RRfile.open(filename, 'r')

    data = f.revread()
    while rval == date_parse._fail and data != '':
        rval = date_parse(data, -1)
        data = f.revread()

    f.close()
    return rval


# -----------------------------------------------------------------------------
def date_start(filename):
    """
    Read filename and return the first timestamp.
    """
    f = open(filename, 'r')
    # initialize rval to date_parse._fail so the while condition will start out
    # being true
    rval = date_parse('', 0)
    line = f.readline()
    while rval == date_parse._fail and line != '':
        rval = date_parse(line, 0)
        line = f.readline()

    f.close()
    return rval


# -----------------------------------------------------------------------------
def rstring():
    """
    Return a string of random characters of a random length between 10 and 25
    """
    size = random.randint(10, 25)
    rval = ''
    for jdx in range(size):
        rval += random.choice(string.uppercase +
                              string.lowercase +
                              string.digits)
    return rval


# -----------------------------------------------------------------------------
def write_file(filename, mode=0644, content=None):
    """
    Write a file, optionally setting its permission bits. This should be in
    util.py.
    """
    f = open(filename, 'w')
    if type(content) == str:
        f.write(content)
    elif type(content) == list:
        f.writelines([x.rstrip() + '\n' for x in content])
    else:
        raise StandardError("content is not of a suitable type (%s)"
                            % type(content))
    f.close()
    os.chmod(filename, mode)


# -----------------------------------------------------------------------------
def ymdhms(epoch, fmt="%Y.%m%d %H:%M:%S"):
    """
    Format an epoch time into YYYY.MMDD HH:MM:SS.
    """
    return time.strftime(fmt, time.localtime(epoch))


# -----------------------------------------------------------------------------
class HpssicError(Exception):
    def __init__(self, value):
        """
        Set up the value, normally a text string
        """
        self.value = value

    def __str__(self):
        """
        Return the value as a string
        """
        return repr(self.value)
