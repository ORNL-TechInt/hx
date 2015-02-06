"""
Configuration class for crawl.py

This class is based on python's standard ConfigParser class. It adds

    1) a function to manage and return a singleton config object (add_config)

    2) sensitivity to updates to the underlying configuration file (changed)

    3) a 'time' type which shows up in the configuration file as '10 sec',
    '2hr', '7 minutes', etc., but is presented to the caller as a number of
    seconds.

    4) a boolean handler which returns False if the option (or section) does
    not exist (rather than throwing an exception)

"""
import ConfigParser
from ConfigParser import NoSectionError
from ConfigParser import NoOptionError
from ConfigParser import InterpolationMissingOptionError
import logging
import msg
import os
import pdb
import re
import stat
import string
import StringIO
import sys
import time
import util
import util as U
import warnings


# ------------------------------------------------------------------------------
def add_config(filename=None,
               cfg=None,
               dct=None,
               close=False,
               env=None,
               default_filename=None):
    """
    If close and everything else is None, destroy the config and return None

    If close and something else, destroy the config and create a new one based
    on the something else.

    If not close, add whatever is specified to the current config.

    If nothing is specified and we have a current config, return it.

    If nothing is specified and we have no current config, create the default
    config and return it.
    """

    if close:
        if hasattr(get_config, "_config"):
            del get_config._config
        if filename is None and cfg is None and dct is None:
            return None

    if filename:
        if not hasattr(get_config, '_config'):
            get_config._config = defaults()
        get_config._config.read(filename)
        return get_config._config
    elif cfg:
        if hasattr(get_config, '_config'):
            for s in cfg.sections():
                for o in cfg.options(s):
                    get_config._config.set(s, o, cfg.get(s, o))
        else:
            get_config._config = cfg
        return get_config._config
    elif dct:
        if hasattr(get_config, '_config'):
            get_config._config.sum_dict(dct)
        else:
            get_config._config = config.dictor(dct)
        return get_config._config
    elif not close and hasattr(get_config, '_config'):
        return get_config._config

    if env:
        filename = os.getenv(env)
    if filename is None:
        filename = default_filename

    if not os.path.exists(filename):
        raise SystemExit("""
        No configuration found. Please do one of the following:
         - cd to a directory with an appropriate %s file,
         - create %s in the current working directory,
         - set $%s to the path of a valid crawler configuration, or
         - use --cfg to specify a configuration file on the command line.
        """ % (default_filename, default_filename, env))
    elif not os.access(filename, os.R_OK):
        raise HXerror("%s is not readable" % filename)

    rval = defaults()
    rval.read(filename)
    get_config._config = rval
    return rval


# ------------------------------------------------------------------------------
def defaults():
    """
    Return a config object with the standard defaults set
    """
    rval = config({'fire': 'no',
                   'frequency': '3600',
                   'pid': "%05d" % os.getpid(),
                   'heartbeat': '10'})
    return rval


# ------------------------------------------------------------------------------
def get_config(cfname='', reset=False, soft=False):
    """
    @DEPRECATED@
    Open the config file based on cfname, $CRAWL_CONF, or the default, in that
    order. Construct a config object, cache it, and return it. Subsequent
    calls will retrieve the cached object unless reset=True, in which case the
    old object is destroyed and a new one is constructed.

    If reset is True and soft is True, we delete any old cached object but do
    not create a new one.

    Note that values in the default dict passed to config must be strings.
    """
    if reset:
        try:
            del get_config._config
        except AttributeError:
            pass

    try:
        rval = get_config._config
    except AttributeError:
        if soft:
            return None
        if cfname == '':
            envval = os.getenv('CRAWL_CONF')
            if None != envval:
                cfname = envval

        if cfname == '':
            cfname = 'crawl.cfg'

        if not os.path.exists(cfname):
            raise SystemExit("""
            No configuration found. Please do one of the following:
             - cd to a directory with an appropriate crawl.cfg file,
             - create crawl.cfg in the current working directory,
             - set $CRAWL_CONF to the path of a valid crawler configuration, or
             - use --cfg to specify a configuration file on the command line.
            """)
        elif not os.access(cfname, os.R_OK):
            raise StandardError("%s is not readable" % cfname)
        rval = config({'fire': 'no',
                       'frequency': '3600',
                       'heartbeat': '10'})
        rval.read(cfname)
        rval.set('crawler', 'filename', cfname)
        get_config._config = rval
    return rval


# ------------------------------------------------------------------------------
def new_logger(logpath='', cfg=None):
    """
    Return a new logging object for this process. The log file path is derived
    from (in order):

     - logpath if set
     - environment ($CRAWL_LOG)
     - cfg
     - default (/var/log/hpssic.log if writable, else /tmp/hpssic.log)
    """
    # -------------------------------------------------------------------------
    def cfg_get(func, section, option, defval):
        if cfg:
            rval = func(section, option, defval)
        else:
            rval = defval
        return rval

    # -------------------------------------------------------------------------
    def raiseError(record=None):
        raise

    envname = os.getenv('CRAWL_LOG')
    try:
        dcfg = get_config()
    except:
        dcfg = None

    if logpath != '':
        final_logpath = logpath
    elif envname:
        final_logpath = envname
    elif cfg:
        try:
            final_logpath = cfg.get('crawler', 'logpath')
        except NoOptionError:
            final_logpath = U.default_logpath()
        except NoSectionError:
            final_logpath = U.default_logpath()
    elif dcfg:
        try:
            final_logpath = dcfg.get('crawler', 'logpath')
        except NoOptionError:
            final_logpath = U.default_logpath()
        except NoSectionError:
            final_logpath = U.default_logpath()
    else:
        final_logpath = U.default_logpath()

    rval = logging.getLogger('hpssic')
    rval.setLevel(logging.INFO)
    host = util.hostname()

    for h in rval.handlers:
        h.close()
        del h

    if cfg:
        maxBytes = cfg.get_size('crawler', 'logsize', 10*1024*1024)
        backupCount = cfg.get_size('crawler', 'logmax', 5)
        archdir = cfg.get_d('crawler', 'archive_dir',
                            U.pathjoin(U.dirname(final_logpath),
                                       'hpss_log_archive'))
    else:
        maxBytes = 10*1024*1024
        backupCount = 5
        archdir = U.pathjoin(U.dirname(final_logpath), 'hpss_log_archive')

    fh = util.ArchiveLogfileHandler(final_logpath,
                                    maxBytes=maxBytes,
                                    backupCount=backupCount,
                                    archdir=archdir)

    strfmt = "%" + "(asctime)s [%s] " % host + '%' + "(message)s"
    fmt = logging.Formatter(strfmt, datefmt="%Y.%m%d %H:%M:%S")
    fh.setFormatter(fmt)
    fh.handleError = raiseError

    while 0 < len(rval.handlers):
        z = U.pop0(rval.handlers)
        del z
    rval.addHandler(fh)

    rval.info('-' * (55 - len(host)))

    return rval


# -----------------------------------------------------------------------------
def log(*args, **kwargs):
    """
    Manage a singleton logging object. If we already have one, we use it.

    If the caller sets *reopen* or *close*, we close the currently open object
    if any. For *reopen*, we get a new one.

    If there's anything in *args*, we expect args[0] to be a format string with
    subsequent elements matching format specifiers.
    """
    def kwget(kwargs, name, defval):
        if name in kwargs:
            rval = kwargs[name]
        else:
            rval = defval
        return rval

    logpath = kwget(kwargs, 'logpath', '')
    cfg = kwget(kwargs, 'cfg', None)
    close = kwget(kwargs, 'close', False)
    if logpath is None:
        logpath = ''

    if close and hasattr(log, '_logger'):
        while 0 < len(log._logger.handlers):
            h = U.pop0(log._logger.handlers)
            h.close()
            del h
        del log._logger

    if not hasattr(log, '_logger') and (logpath or cfg):
        log._logger = new_logger(logpath=logpath, cfg=cfg)

    if 0 < len(args):
        cframe = sys._getframe(1)
        caller_name = cframe.f_code.co_name
        caller_file = cframe.f_code.co_filename
        caller_lineno = cframe.f_lineno
        fmt = (caller_name +
               "(%s:%d): " % (caller_file, caller_lineno) +
               args[0])
        nargs = (fmt,) + args[1:]
        try:
            log._logger.info(*nargs)
        except AttributeError:
            log._logger = new_logger(logpath=logpath, cfg=cfg)
            log._logger.info(*nargs)

    if hasattr(log, '_logger'):
        return log._logger
    else:
        return None


# ------------------------------------------------------------------------------
def pid_dir():
    """
    Return the path to a directory for storing pid files. We expect it to be in
    the configuration but provide a hard coded default in case it is not.
    """
    cfg = add_config()
    return cfg.get_d('crawler', 'pid_dir', msg.default_piddir)


# ------------------------------------------------------------------------------
class config(ConfigParser.ConfigParser):
    """
    See the module description for information on this class.
    """
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize the object with a default filename and load time.
        """
        self.filename = '<???>'
        self.loadtime = 0.0
        m = sys.modules[__name__]
        m.NoOptionError = m.ConfigParser.NoOptionError
        ConfigParser.ConfigParser.__init__(self, *args, **kwargs)

        if self.has_option('DEFAULT', 'root'):
            root = self.get('DEFAULT', 'root')
            if not root.startswith('/'):
                self.set('DEFAULT', 'root', os.path.abspath(root))

    # -------------------------------------------------------------------------
    def changed(self):
        """
        Return True if the file we were loaded from has changed since load
        time.
        """
        if self.filename != '<???>' and self.loadtime != 0.0:
            s = os.stat(self.filename)
            rval = (self.loadtime < s[stat.ST_MTIME])
        else:
            rval = False
        return rval

    # -------------------------------------------------------------------------
    def dump(self, with_defaults=False):
        """
        Write the contents of the config except for the defaults to a string
        and return the string. If with_defaults = True, include the DEFAULTS
        section.
        """
        rval = ''
        if with_defaults and self.defaults():
            defaults = self.defaults()
            rval += "[DEFAULT]\n"
            for o in defaults:
                rval += "%s = %s\n" % (o, defaults[o])

        for s in self._sections:
            rval += '\n[%s]\n' % s
            for o in self._sections[s]:
                val = self.get(s, o)
                rval += '%s = %s\n' % (o, val)
        return rval

    # -------------------------------------------------------------------------
    def get_d(self, section, option, default='get_d.nothing'):
        """
        Return the section/option value from the config. If the section or
        option is not defined, return the default value. If the default value
        is None, pass the exception up the stack, but add the config filename
        to it first.
        """
        try:
            value = self.get(section, option)
        except ConfigParser.NoSectionError as e:
            if default != 'get_d.nothing':
                value = default
            else:
                e.message += " in %s" % self.filename
                raise
        except ConfigParser.NoOptionError as e:
            if default != 'get_d.nothing':
                value = default
            else:
                e.message += " in %s" % self.filename
                raise
        return value

    # -------------------------------------------------------------------------
    def get_size(self, section, option, default=None):
        """
        Unit specs are case insensitive.

        b  -> 1
        kb -> 1000**1       kib -> 1024
        mb -> 1000**2       mib -> 1024**2
        gb -> 1000**3       gib -> 1024**3
        tb -> 1000**4       tib -> 1024**4
        pb -> 1000**5       pib -> 1024**5
        eb -> 1000**6       eib -> 1024**6
        zb -> 1000**7       zib -> 1024**7
        yb -> 1000**8       yib -> 1024**8
        """
        try:
            spec = self.get(section, option)
            rval = util.scale(spec)
        except ConfigParser.NoOptionError as e:
            if default is not None:
                rval = default
            else:
                e.message += " in %s" % self.filename
                raise
        except ConfigParser.NoSectionError as e:
            if default is not None:
                rval = default
            else:
                e.message += " in %s" % self.filename
                raise

        return rval

    # -------------------------------------------------------------------------
    def get_time(self, section, option, default=None):
        """
        Retrieve the value of section/option. It is assumed to be a duration
        specification, like -- '10 seconds', '2hr', '7 minutes', or the like.

        We will call map_time_unit to convert the unit into a number of
        seconds, then multiply by the magnitude, and return an int number of
        seconds. If the caller specifies a default and we get a NoSectionError
        or NoOptionError, we will return the caller's default. Otherwise, we
        raise the exception.
        """
        # ---------------------------------------------------------------------
        def handle_exception(exc, defval):
            if type(defval) == int:
                rval = defval
                # log(str(e) + '; using default value %d' % defval)
            elif type(defval) == float:
                rval = defval
                # log(str(e) + '; using default value %f' % defval)
            elif defval is not None:
                raise U.HXerror(msg.default_int_float)
            else:
                exc.message += " in %s" % self.filename
                raise
            return rval

        # ---------------------------------------------------------------------
        try:
            spec = self.get(section, option)
            rval = self.to_seconds(spec)
        except ConfigParser.NoOptionError as e:
            rval = handle_exception(e, default)
        except ConfigParser.NoSectionError as e:
            rval = handle_exception(e, default)

        return rval

    # -------------------------------------------------------------------------
    def to_seconds(self, spec):
        """
        Convert a time spec like '10min' to seconds
        """
        if spec.strip()[0] not in string.digits + '.':
            raise ValueError(msg.invalid_time_mag_S % spec)
        [(mag, unit)] = re.findall('([\d.]+)\s*(\w*)', spec)
        mult = self.map_time_unit(unit)
        rval = int(float(mag) * mult)
        return rval

    # -------------------------------------------------------------------------
    def getboolean(self, name, option):
        """
        Retrieve the value of section(name)/option as a boolean. If the option
        does not exist, catch the exception and return False.
        """
        try:
            rval = ConfigParser.ConfigParser.getboolean(self, name, option)
        except ValueError:
            rval = False
        except ConfigParser.NoOptionError as e:
            rval = False
        except ConfigParser.NoSectionError as e:
            rval = False
        return rval

    # -------------------------------------------------------------------------
    @classmethod
    def dictor(cls, dict, defaults=None):
        """
        Constructor initialized from a dict. If something is passed to
        *defaults*, it will be forwarded to __init__() as the defaults dict for
        initializing the _defaults member of the object.
        """
        rval = config(defaults=defaults)

        # Now fill the config with the material from the dict
        for s in sorted(dict.keys()):
            rval.add_section(s)
            for o in sorted(dict[s].keys()):
                rval.set(s, o, dict[s][o])
        return rval

    # -------------------------------------------------------------------------
    def sum_dict(self, dct, defaults=None):
        """
        Add the contents of *dct* to self. If one of the keys in dict is
        'defaults' or 'DEFAULTS', that sub-dict will be added to the _defaults
        member. The new material may overwrite what's already present.
        """
        # If we got defaults, set them first
        if defaults is not None:
            for k in defaults.keys():
                self._defaults[k] = defaults[k]

        # Now fill the config with the material from the dict
        for s in sorted(dct.keys()):
            if not self.has_section(s):
                self.add_section(s)
            for o in sorted(dct[s].keys()):
                self.set(s, o, dct[s][o])

    # -------------------------------------------------------------------------
    def map_time_unit(self, spec):
        """
        1s         => 1
        1 min      => 60
        2 days     => 2 * 24 * 3600
        """
        done = False
        while not done:
            try:
                rval = self._map[spec]
                done = True
            except AttributeError:
                self._map = {'': 1,
                             's': 1,
                             'sec': 1,
                             'second': 1,
                             'seconds': 1,
                             'm': 60,
                             'min': 60,
                             'minute': 60,
                             'minutes': 60,
                             'h': 3600,
                             'hr': 3600,
                             'hour': 3600,
                             'hours': 3600,
                             'd': 24 * 3600,
                             'day': 24 * 3600,
                             'days': 24 * 3600,
                             'w': 7 * 24 * 3600,
                             'week': 7 * 24 * 3600,
                             'weeks': 7 * 24 * 3600,
                             'month': 30 * 24 * 3600,
                             'months': 30 * 24 * 3600,
                             'y': 365 * 24 * 3600,
                             'year': 365 * 24 * 3600,
                             'years': 365 * 24 * 3600,
                             }
                done = False
            except KeyError:
                raise ValueError(msg.invalid_time_unit_S % spec)

        return rval

    # -------------------------------------------------------------------------
    @classmethod
    def meta_section(cls):
        """
        Set the meta section name where filename and any other metadata for the
        configuration will be held
        """
        return 'cfgmeta'

    # -------------------------------------------------------------------------
    def db_section(self):
        """
        Return a section describing a database if present or throw an exception
        otherwise
        """
        sl = self.sections()
        m = self.meta_section()
        if m in sl:
            sl.remove(m)

        db_l = [x for x in sl if self.has_option(x, 'dbtype')]
        if 0 == len(db_l):
            raise U.HXerror(msg.missing_db_section)
        return db_l[0]

    # -------------------------------------------------------------------------
    def qt_parse(self, spec):
        """
        Build and return a dict representing a quiet time period. It may be

         - a pair of offsets into the day, with the quiet time between the
           offsets. There are two possibilities: low < high, high < low -
           eg: low=50, hi=100 means that (day+lo) <= qt <= (day+hi).
           otoh: hi < lo means that
              (day+1o) <= qt <= (day+23:59:59) and/or
              (day) <= qt <= (day+hi)

         - a weekday. in this case, if time.localtime(now)[6] == wday, we're in
           quiet time.

         - a date. in this case, we'll set low = day and high = (day+23:59:59)

        The returned dict will have the following keys:
         - 'lo': times after this are quiet
         - 'hi': times before this are quiet
         - 'base': for a date, this is the epoch. for a weekday, 0 = mon, ...
         - 'iter': for a date, this is 0. for a weekday, it's 604800
        """
        rval = {}
        dow_s = ' monday tuesday wednesday thursday friday saturday sunday'
        wday_d = dict(zip(dow_s.strip().split(), range(7)))

        try:
            ymd_tm = time.strptime(spec, "%Y.%m%d")
        except ValueError:
            ymd_tm = False

        hm_l = re.findall("(\d+):(\d+)", spec)
        if (2 != len(hm_l)) or (2 != len(hm_l[0])) or (2 != len(hm_l[1])):
            hm_l = []

        if " " + spec.lower() in dow_s:
            # we have a week day
            [wd] = [x for x in wday_d.keys() if spec.lower() in x]

            rval['spec'] = spec
            rval['lo'] = 0.0
            rval['hi'] = 24*3600.0 - 1
            rval['base'] = wday_d[wd]
            rval['iter'] = 24 * 3600.0 * 7

        elif ymd_tm:
            # we have a date
            rval['spec'] = spec
            rval['lo'] = 0
            rval['hi'] = 24 * 3600.0 - 1
            rval['base'] = time.mktime(ymd_tm)
            rval['iter'] = 0

        elif hm_l:
            # we have a time range
            rval['spec'] = spec
            rval['lo'] = 60.0 * (60.0 * int(hm_l[0][0]) + int(hm_l[0][1]))
            rval['hi'] = 60.0 * (60.0 * int(hm_l[1][0]) + int(hm_l[1][1]))
            rval['base'] = -1
            rval['iter'] = 24 * 3600.0

        else:
            raise StandardError("qt_parse fails on '%s'" % spec)

        return rval

    # -------------------------------------------------------------------------
    def quiet_time(self, when):
        """
        Config setting crawler/quiet_time may contain a comma separated list of
        time interval specifications. For example:

           17:00-19:00      (5pm to 7pm)
           20:00-03:00      (8pm to the folliwng 3am)
           sat              (00:00:00 to 23:59:59 every Saturday)
           2014.0723        (00:00:00 to 23:59:59 on 2014.0723)
           14:00-17:00,fri  (2pm to 5pm and all day Friday)
        """
        rval = False
        try:
            x = self._qt_list
        except AttributeError:
            self._qt_list = []
            if self.has_option('crawler', 'quiet_time'):
                spec = self.get('crawler', 'quiet_time')
                for ispec in util.csv_list(spec):
                    self._qt_list.append(self.qt_parse(ispec))

        for x in self._qt_list:
            if x['iter'] == 0:
                # it's a date
                low = x['base'] + x['lo']
                high = x['base'] + x['hi']
                if low <= when and when <= high:
                    rval = True

            elif x['iter'] == 24 * 3600.0:
                # it's a time range

                db = util.daybase(when)
                low = db + x['lo']
                high = db + x['hi']
                dz = db + 24 * 3600.0
                if low < high:
                    # right side up
                    if low <= when and when <= high:
                        rval = True
                elif high < low:
                    # up side down
                    if db <= when and when <= high:
                        rval = True
                    elif low <= when and when <= dz:
                        rval = True
                else:
                    # low and high are equal -- log a note
                    log("In time spec '%s', the times are equal " % x['spec'] +
                        "so the interval is almost empty. This may not be " +
                        "what you intended")
                    if when == low:
                        rval = True

            elif x['iter'] == 24 * 3600.0 * 7:
                # it's a weekday
                tm = time.localtime(when)
                if tm.tm_wday == x['base']:
                    rval = True

            else:
                # something bad happened
                raise StandardError("Hell has frozen over")

        return rval

    # -------------------------------------------------------------------------
    def read(self, filename):
        """
        Read the configuration file and cache the file name and load time. Also
        read any included config files.
        """
        ConfigParser.ConfigParser.readfp(self, open(filename), filename)
        self.filename = filename
        self.loadtime = time.time()

        pending = self.update_include_list()   # creates dict self.incl
        while pending != []:
            parsed = ConfigParser.ConfigParser.read(self, pending)
            for fname in pending:
                self.incl[fname] = True
            unparsed = [x for x in pending if x not in parsed]
            if unparsed != []:
                wmsg = "Some config files not loaded: %s" % ", ".join(unparsed)
                warnings.warn(wmsg)
            pending = self.update_include_list()   # update dict self.incl

        try:
            self.set(self.meta_section(), 'filename', filename)
        except NoSectionError:
            self.add_section(self.meta_section())
            self.set(self.meta_section(), 'filename', filename)

    # -------------------------------------------------------------------------
    def update_include_list(self):
        """
        Rummage through the config object and find all the 'include' options.
        They are added to a dict of the form

             {<filename>: <boolean>, ... }

        If <boolean> is False, the file has not yet been parsed into the config
        object. Once it is, <boolean> is set to True. We return the list of
        pending files to included, i.e., those that have not yet been.
        """
        if not hasattr(self, 'incl'):
            setattr(self, 'incl', {})

        for section in self.sections():
            for option in self.options(section):
                if option == 'include':
                    if self.get(section, option) not in self.incl:
                        self.incl[self.get(section, option)] = False

        return [x for x in self.incl if not self.incl[x]]

    # -------------------------------------------------------------------------
    def crawl_write(self, fp):
        """
        Write the config material to fp with the 'crawler' section first. fp
        must be an already open file descriptor. If there is no 'crawler'
        section, raise a NoSectionError.
        """
        if 'crawler' not in self.sections():
            raise StandardError("section 'crawler' missing from test config")

        # move 'crawler' to the beginning of the section list
        section_l = self.sections()
        section_l.remove('crawler')
        section_l = ['crawler'] + section_l

        for section in section_l:
            fp.write("[%s]\n" % section)
            for item in self.options(section):
                if 'include' == item:
                    continue
                val = self.get(section, item).replace("\n", "")
                fp.write("%s = %s\n" % (item, val))
            fp.write("\n")
