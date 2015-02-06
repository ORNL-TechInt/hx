"""
Test class for cfg.py
"""
import contextlib as ctx
import copy
import hx.msg
import hx.testhelp
import hx.util as U
import logging
import os
import pdb
import pytest
import re
import sys
import time
import warnings


# -----------------------------------------------------------------------------
class cfg_Test(hx.testhelp.HelpedTestCase):
    """
    Test class for cfg
    """
    default_cfname = 'crawl.cfg'
    env_cfname = 'envcrawl.cfg'
    exp_cfname = 'explicit.cfg'
    cdict = {'crawler': {'logsize': '5mb',
                         'logmax': '5',
                         'e-mail-recipients':
                         'tbarron@ornl.gov, tusculum@gmail.com',
                         'trigger': '<command-line>',
                         'plugins': 'plugin_A',
                         },
             'plugin_A': {'frequency': '1h',
                          'operations': '15'
                          }
             }
    sample = {'crawler': {'opt1': 'foo',
                          'opt2': 'fribble',
                          'opt3': 'nice',
                          'heartbeat': '1hr',
                          'frequency': '5 min'},
              'sounds': {'duck': 'quack',
                         'dog':  'bark',
                         'hen':  'cluck'}}
    nocfg_exp = """
            No configuration found. Please do one of the following:
             - cd to a directory with an appropriate crawl.cfg file,
             - create crawl.cfg in the current working directory,
             - set $CRAWL_CONF to the path of a valid crawler configuration, or
             - use --cfg to specify a configuration file on the command line.
            """

    # -------------------------------------------------------------------------
    def test_changed(self):
        """
        Routines exercised: __init__(), changed(), and crawl_write()
        """
        self.dbgfunc()
        cfgfile = self.tmpdir('test_changed.cfg')

        obj = hx.cfg.config.dictor(self.sample)
        f = open(cfgfile, 'w')
        obj.crawl_write(f)
        f.close()

        changeable = hx.cfg.config()
        self.expected('<???>', changeable.filename)
        self.expected(0.0, changeable.loadtime)
        changeable.read(cfgfile)
        self.assertFalse(changeable.changed())
        os.utime(cfgfile, (time.time() + 5, time.time() + 5))
        self.assertTrue(changeable.changed())
        self.expected(cfgfile, changeable.filename)

    # -------------------------------------------------------------------------
    def test_dictor(self):
        """
        Test loading a config object from a dict with config.dictor()
        """
        cfg = hx.cfg.config.dictor(self.cdict)
        for sect in self.cdict:
            for opt in self.cdict[sect]:
                self.expected(self.cdict[sect][opt], cfg.get(sect, opt))

    # -------------------------------------------------------------------------
    def test_dictor_alt(self):
        """
        Routines exercised: dictor()
        """
        obj = hx.cfg.config.dictor(self.sample)

        self.expected('<???>', obj.filename)
        self.expected(0.0, obj.loadtime)

        self.expected_in('crawler', obj.sections())
        self.expected_in('sounds', obj.sections())

        self.expected_in('opt1', obj.options('crawler'))
        self.expected_in('opt2', obj.options('crawler'))
        self.expected_in('opt3', obj.options('crawler'))

        self.expected_in('duck', obj.options('sounds'))
        self.expected_in('dog', obj.options('sounds'))
        self.expected_in('hen', obj.options('sounds'))

    # -------------------------------------------------------------------------
    def test_dump_nodef(self):
        """
        Routines exercised: __init__(), dump().
        """
        obj = hx.cfg.config.dictor(self.sample,
                                   defaults={'goose': 'honk'})
        dumpstr = obj.dump()

        self.assertFalse("[DEFAULT]" in dumpstr)
        self.assertFalse("goose = honk" in dumpstr)
        self.expected_in("[crawler]", dumpstr)
        self.expected_in("opt1 = foo", dumpstr)
        self.expected_in("opt2 = fribble", dumpstr)
        self.expected_in("opt3 = nice", dumpstr)
        self.expected_in("[sounds]", dumpstr)
        self.expected_in("dog = bark", dumpstr)
        self.expected_in("duck = quack", dumpstr)
        self.expected_in("hen = cluck", dumpstr)

    # -------------------------------------------------------------------------
    def test_dump_withdef(self):
        """
        Routines exercised: __init__(), dump().
        """
        defaults = {'goose': 'honk'}
        obj = hx.cfg.config.dictor(self.sample, defaults=defaults)
        dumpstr = obj.dump(with_defaults=True)

        self.expected_in("[DEFAULT]", dumpstr)
        self.expected_in("goose = honk", dumpstr)
        self.expected_in("[crawler]", dumpstr)
        self.expected_in("opt1 = foo", dumpstr)
        self.expected_in("opt2 = fribble", dumpstr)
        self.expected_in("opt3 = nice", dumpstr)
        self.expected_in("[sounds]", dumpstr)
        self.expected_in("dog = bark", dumpstr)
        self.expected_in("duck = quack", dumpstr)
        self.expected_in("hen = cluck", dumpstr)

    # --------------------------------------------------------------------------
    def test_get_config_def_noread(self):
        """
        TEST: env not set, 'crawl.cfg' does exist but not readable

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None)):
            self.write_cfg_file(self.default_cfname, self.cdict)
            os.chmod(self.default_cfname, 0000)

            expmsg = "%s is not readable" % self.default_cfname
            # test get_config with no argument
            self.assertRaisesMsg(StandardError, expmsg, hx.cfg.get_config)

            # test get_config with empty string argument
            self.assertRaisesMsg(StandardError, expmsg,
                                 hx.cfg.get_config, '')

    # --------------------------------------------------------------------------
    def test_get_config_def_nosuch(self):
        """
        TEST: env not set, 'crawl.cfg' does not exist

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()), U.tmpenv('CRAWL_CONF', None)):
            U.conditional_rm(self.default_cfname)

            # test with no argument
            self.assertRaisesMsg(SystemExit, self.nocfg_exp,
                                 hx.cfg.get_config)

            # test with empty string argument
            self.assertRaisesMsg(SystemExit, self.nocfg_exp,
                                 hx.cfg.get_config, '')

    # --------------------------------------------------------------------------
    def test_get_config_def_ok(self):
        """
        TEST: env not set, 'crawl.cfg' does exist =>

        EXP: get_config() or get_config('') should load the config
        """
        self.dbgfunc()
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()), U.tmpenv('CRAWL_CONF', None)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.default_cfname
            d['crawler']['logpath'] = self.logpath()
            self.write_cfg_file(self.default_cfname, d)
            os.chmod(self.default_cfname, 0644)

            cfg = hx.cfg.get_config()
            self.expected(self.default_cfname, cfg.get('crawler', 'filename'))
            self.expected(self.default_cfname, cfg.filename)

            cfg = hx.cfg.get_config('')
            self.expected(self.default_cfname, cfg.get('crawler', 'filename'))
            self.expected(self.default_cfname, cfg.filename)

    # --------------------------------------------------------------------------
    def test_get_config_env_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists but
        is not readable

        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.env_cfname
            self.write_cfg_file(self.env_cfname, d)
            os.chmod(self.env_cfname, 0000)

            expmsg = "%s is not readable" % self.env_cfname
            self.assertRaisesMsg(StandardError, expmsg,
                                 hx.cfg.get_config)

            self.assertRaisesMsg(StandardError, expmsg,
                                 hx.cfg.get_config, '')

    # --------------------------------------------------------------------------
    def test_get_config_env_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg does not exist

        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            U.conditional_rm(self.env_cfname)

            self.assertRaisesMsg(SystemExit,
                                 self.nocfg_exp,
                                 hx.cfg.get_config)

            self.assertRaisesMsg(SystemExit,
                                 self.nocfg_exp,
                                 hx.cfg.get_config,
                                 '')

    # --------------------------------------------------------------------------
    def test_get_config_env_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
        is readable

        EXP: get_config(), get_config('') should load the config
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.env_cfname
            self.write_cfg_file(self.env_cfname, d)
            os.chmod(self.env_cfname, 0644)

            cfg = hx.cfg.get_config()
            self.expected(self.env_cfname, cfg.get('crawler', 'filename'))

            cfg = hx.cfg.get_config('')
            self.expected(self.env_cfname, cfg.get('crawler', 'filename'))

    # --------------------------------------------------------------------------
    def test_get_config_exp_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, unreadable explicit.cfg exists

        EXP: get_config('explicit.cfg') should should throw a
             StandardError about the file not existing or not being
             readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.env_cfname
            self.write_cfg_file(self.env_cfname, d)
            os.chmod(self.env_cfname, 0644)

            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.exp_cfname
            self.write_cfg_file(self.exp_cfname, d)
            os.chmod(self.exp_cfname, 0000)

            self.assertRaisesMsg(StandardError,
                                 "%s is not readable" % self.exp_cfname,
                                 hx.cfg.get_config,
                                 self.exp_cfname)

    # --------------------------------------------------------------------------
    def test_get_config_exp_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
              is readable, explicit.cfg does not exist

        EXP: get_config('explicit.cfg') should throw a StandardError
             about the file not existing or not being readable
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.env_cfname
            self.write_cfg_file(self.env_cfname, d)
            os.chmod(self.env_cfname, 0644)

            U.conditional_rm(self.exp_cfname)

            self.assertRaisesMsg(SystemExit,
                                 self.nocfg_exp,
                                 hx.cfg.get_config,
                                 self.exp_cfname)

    # --------------------------------------------------------------------------
    def test_get_config_exp_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, readable explicit.cfg does exist

        EXP: get_config('explicit.cfg') should load the explicit.cfg
        """
        hx.cfg.get_config(reset=True, soft=True)
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', self.env_cfname)):
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.env_cfname
            self.write_cfg_file(self.env_cfname, d)
            os.chmod(self.env_cfname, 0644)

            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.exp_cfname
            self.write_cfg_file(self.exp_cfname, d)
            os.chmod(self.exp_cfname, 0644)

            cfg = hx.cfg.get_config(self.exp_cfname)
            self.expected(self.exp_cfname, cfg.get('crawler', 'filename'))

    # -------------------------------------------------------------------------
    def test_get_d_none(self):
        """
        Changing get_d() to support None as a default value
        """
        obj = hx.cfg.config.dictor(self.sample)

        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck', 'foobar'))

        # section is defined, option is not, should get the default
        self.expected(None, obj.get_d('sounds', 'dolphin', None))

        # section defined, option not, no default -- should throw exception
        self.assertRaisesMsg(hx.cfg.NoOptionError,
                             "No option '%s' in section: '%s' in <???>" %
                             ("dolphin", "sounds"),
                             obj.get_d,
                             'sounds',
                             'dolphin')

        # section not defined, should get the default
        self.expected(None, obj.get_d('malename', 'deer', None))

        # section not defined, no default -- should throw exception
        self.assertRaisesMsg(hx.cfg.NoSectionError,
                             "No section: 'malename' in <???>",
                             obj.get_d,
                             'malename',
                             'deer')

    # -------------------------------------------------------------------------
    def test_get_d_with(self):
        """
        Calling get_d() with a default value should

          1) return the option value if it's defined
          2) return the default value otherwise

        If a default value is provided, get_d should not throw NoOptionError or
        NoSectionError
        """
        obj = hx.cfg.config.dictor(self.sample)
        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck', 'foobar'))
        # section is defined, option is not, should get the default
        self.expected('whistle', obj.get_d('sounds', 'dolphin', 'whistle'))
        # section not defined, should get the default
        self.expected('buck', obj.get_d('malename', 'deer', 'buck'))

    # -------------------------------------------------------------------------
    def test_get_d_without_opt(self):
        """
        Calling get_d() without a default value should

          1) return the option value if it's defined
          2) otherwise throw a NoOptionError when the option is missing
        """
        obj = hx.cfg.config.dictor(self.sample)
        fname = U.my_name()
        obj.filename = fname
        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck'))

        # section is defined, option is not, should get exception
        self.assertRaisesMsg(hx.cfg.NoOptionError,
                             "No option '%s' in section: '%s' in %s" %
                             ("dolphin", "sounds", fname),
                             obj.get_d,
                             'sounds',
                             'dolphin')

    # -------------------------------------------------------------------------
    def test_get_d_without_sect(self):
        """
        Calling get_d() without a default value should

          1) return the option value if it's defined
          2) otherwise throw a NoSectionError or NoOptionError
        """
        obj = hx.cfg.config.dictor(self.sample)
        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck'))

        # section not defined, should get NoSectionError
        self.assertRaisesMsg(hx.cfg.NoSectionError,
                             "No section: 'malename' in <???>",
                             obj.get_d,
                             'malename',
                             'deer')

    # -------------------------------------------------------------------------
    def test_logging_00(self):
        """
        With no logger cached, calling config.log() with a logpath and
        close=True should create a new logger. If a logger has been created,
        a call with close=False should not open a new logpath.
        """
        self.dbgfunc()
        # throw away any logger that has been set
        # and get a new one
        trg_logpath = self.tmpdir('hx.cfg.log')
        exp_logpath = U.abspath(trg_logpath)
        actual = hx.cfg.log(logpath=trg_logpath, close=True)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(exp_logpath, actual.handlers[0].baseFilename)

        # now ask for a logger with a different name, with close=False. Since
        # one has already been created, the new name should be ignored and we
        # should get back the one already cached.
        actual = hx.cfg.log(logpath=self.tmpdir('util_foobar.log'))
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(exp_logpath, actual.handlers[0].baseFilename)

    # -------------------------------------------------------------------------
    def test_logging_01(self):
        """
        With no logger cached, close=True should not create a new logger. If a
        logger has been created, calling config.log() with no arguments
        should return the cached logger.
        """
        # reset the logger
        actual = hx.cfg.log(close=True)
        self.expected(None, actual)

        # now create a logger
        trg_logpath = self.tmpdir('hx.cfg.log')
        exp_logpath = U.abspath(trg_logpath)
        hx.cfg.log(logpath=trg_logpath)

        # now retrieving the logger should get the one just set
        actual = hx.cfg.log()
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(exp_logpath, actual.handlers[0].baseFilename)

    # -------------------------------------------------------------------------
    def test_logging_10(self):
        """
        Calling config.log() with close=True and a logpath should get rid
        of the previously cached logger and make a new one.
        """
        # throw away any logger that has been set and create one to be
        # overridden
        throwaway = self.tmpdir('throwaway.log')
        abs_throwaway = U.abspath(throwaway)

        override = self.tmpdir('hx.cfg.log')
        abs_override = U.abspath(override)

        tmp = hx.cfg.log(logpath=throwaway, close=True)

        # verify that it's there with the expected attributes
        self.assertTrue(isinstance(tmp, logging.Logger),
                        "Expected logging.Logger, got %s" % (tmp))
        self.expected(1, len(tmp.handlers))
        self.expected(abs_throwaway, tmp.handlers[0].baseFilename)

        # now override it
        actual = hx.cfg.log(logpath=override, close=True)

        # and verify that it got replaced
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(1, len(actual.handlers))
        self.expected(abs_override, actual.handlers[0].baseFilename)

    # -------------------------------------------------------------------------
    def test_logging_11(self):
        """
        Calling config.log() with close=True and no new logpath should
        throw away any cached logger and return None without creating a new
        one.
        """
        exp = None
        actual = hx.cfg.log(close=True)
        self.expected(exp, actual)

    # -------------------------------------------------------------------------
    def test_logging_cfg(self):
        """
        Call config.log() with a config that specifies non default values
        for log file name, log file size, and max log files on disk. Verify
        that the resulting logger has the correct parameters.
        """
        cfname = self.tmpdir("%s.cfg" % U.my_name())
        lfname = self.tmpdir("%s.log" % U.my_name())
        cdict = {'crawler': {'logpath': lfname,
                             'logsize': '17mb',
                             'logmax': '13'
                             }
                 }
        c = hx.cfg.config.dictor(cdict)

        # reset any logger that has been initialized and ask for one that
        # matches the configuration
        l = hx.cfg.log(cfg=c, close=True)

        # and check that it has the right handler
        self.assertNotEqual(l, None)
        self.expected(1, len(l.handlers))
        self.expected(os.path.abspath(lfname), l.handlers[0].stream.name)
        self.expected(17*1000*1000, l.handlers[0].maxBytes)
        self.expected(13, l.handlers[0].backupCount)

        self.assertPathPresent(lfname)

    # -------------------------------------------------------------------------
    def test_logging_def_cfg(self):
        """
        Call config.log() with no logpath or cfg arguments but with a
        default config file available. The result should be a logger open on
        the log path named in the default config file (retrieved by
        config.get_config()).
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()), U.tmpenv('CRAWL_CONF', None)):
            # reset any logger that has been initialized
            hx.cfg.log(close=True)
            hx.cfg.get_config(reset=True, soft=True)

            logpath = os.path.basename(self.logpath())
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.default_cfname
            d['crawler']['logpath'] = logpath
            self.write_cfg_file(self.default_cfname, d)
            os.chmod(self.default_cfname, 0644)

            # now ask for a default logger
            l = hx.cfg.log("frooble test log")

            # and check that it has the right handler
            self.expected(1, len(l.handlers))
            self.expected(os.path.abspath(logpath),
                          l.handlers[0].stream.name)
            self.expected(10*1024*1024, l.handlers[0].maxBytes)
            self.expected(5, l.handlers[0].backupCount)

    # --------------------------------------------------------------------------
    def test_logging_default(self):
        """
        TEST: Call config.log() with no argument

        EXP: Attempts to log to '/var/log/crawl.log', falls back to
        '/tmp/crawl.log' if we can't access the protected file
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()), U.tmpenv('CRAWL_CONF', None)):
            U.conditional_rm('crawl.cfg')
            hx.cfg.get_config(reset=True, soft=True)
            hx.cfg.log(close=True)
            lobj = hx.cfg.log("This is a test log message")
            self.expected(U.default_logpath(), lobj.handlers[0].stream.name)

    # -------------------------------------------------------------------------
    def test_logging_nocfg(self):
        """
        Call config.log() with no cmdline or cfg arguments and make sure
        the resulting logger has the correct parameters.
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()), U.tmpenv('CRAWL_CONF', None)):
            # reset any logger and config that has been initialized
            hx.cfg.get_config(reset=True, soft=True)
            hx.cfg.log(close=True)

            # now ask for a default logger
            l = hx.cfg.log("test log message")

            # and check that it has the right handler
            self.expected(1, len(l.handlers))
            self.expected(U.default_logpath(), l.handlers[0].stream.name)
            self.expected(10*1024*1024, l.handlers[0].maxBytes)
            self.expected(5, l.handlers[0].backupCount)

    # --------------------------------------------------------------------------
    def test_logging_path(self):
        """
        TEST: Call config.log() with a pathname

        EXP: Attempts to log to pathname
        """
        hx.cfg.log(close=True)
        logpath = '%s/%s.log' % (self.tmpdir(), U.my_name())
        U.conditional_rm(logpath)
        self.assertPathNotPresent(logpath)
        lobj = hx.cfg.log(logpath=logpath)
        self.assertPathPresent(logpath)

    # -------------------------------------------------------------------------
    def test_get_size(self):
        """
        Routine get_size() translates expressions like '30 mib' to 30 * 1024 *
        1024 or '10mb' to 10,000,000
        """
        section = U.my_name()
        obj = hx.cfg.config()
        obj.add_section(section)
        obj.set(section, 'tenmb', '10mb')
        obj.set(section, 'thirtymib', '30mib')
        obj.set(section, 'bogusunit', '92thousand')
        self.expected(10*1000*1000, obj.get_size(section, 'tenmb'))
        self.expected(30*1024*1024, obj.get_size(section, 'thirtymib'))
        self.expected(92, obj.get_size(section, 'bogusunit'))

    # -------------------------------------------------------------------------
    def test_get_size_opt_def(self):
        """
        Call get_size() so it throws NoOptionError but provides a default value
        """
        section = U.my_name()
        obj = hx.cfg.config()
        obj.add_section(section)
        obj.set(section, 'tenmb', '10mb')
        obj.set(section, 'thirtymib', '30mib')
        self.expected(17, obj.get_size(section, 'foobar', 17))

    # -------------------------------------------------------------------------
    def test_get_size_opt(self):
        """
        Call get_size() in such a way that it throws a NoOptionError so we can
        see that it adds the filename to the error message
        """
        section = U.my_name()
        fpath = __file__
        obj = hx.cfg.config()
        obj.add_section(section)
        obj.filename = fpath
        self.assertRaisesMsg(hx.cfg.NoOptionError,
                             "No option 'foobar' in section: '%s' in %s" %
                             (section, fpath),
                             obj.get_size,
                             section,
                             "foobar")

    # -------------------------------------------------------------------------
    def test_get_size_sect(self):
        """
        Call get_size() in such a way that it throws a NoSectionError so we can
        see that it adds the filename to the error message
        """
        section = U.my_name()
        obj = hx.cfg.config()
        self.assertRaisesMsg(hx.cfg.NoSectionError,
                             "No section: '%s' in <???>" %
                             section,
                             obj.get_size,
                             section,
                             "foobar")

    # -------------------------------------------------------------------------
    def test_get_size_sect_def(self):
        """
        Call get_size() so it throws NoSectionError but provides a default
        value
        """
        section = U.my_name()
        obj = hx.cfg.config()
        obj.add_section(section)
        obj.set(section, 'tenmb', '10mb')
        obj.set(section, 'thirtymib', '30mib')
        self.expected(17, obj.get_size(section + "nosuch", 'foobar', 17))

    # -------------------------------------------------------------------------
    def test_get_time(self):
        """
        Routines exercised: __init__(), get_time().
        """
        obj = hx.cfg.config.dictor(self.sample)
        self.expected(3600, obj.get_time('crawler', 'heartbeat'))
        self.expected(300, obj.get_time('crawler', 'frequency'))

    # -------------------------------------------------------------------------
    def test_get_time_float(self):
        """
        Verify that get_time handles floats correctly
        """
        self.dbgfunc()
        obj = hx.cfg.config.dictor({'crawler': {'dsec': '10s',
                                                'fsec': '5.0 s',
                                                'nzmin': '.25min',
                                                'lzhour': '0.1 hr',
                                                'fday': '7.0d',
                                                },
                                    })
        self.expected(10, obj.get_time('crawler', 'dsec'))
        self.expected(5, obj.get_time('crawler', 'fsec'))
        self.expected(15, obj.get_time('crawler', 'nzmin'))
        self.expected(360, obj.get_time('crawler', 'lzhour'))
        self.expected(7*24*3600, obj.get_time('crawler', 'fday'))

    # -------------------------------------------------------------------------
    def test_get_time_invalid(self):
        """
        Verify that get_time recognizes bad time specs and handles them
        properly
        """
        self.dbgfunc()
        td = {'punct': {'val': '75%8 fla',
                        'msg': hx.msg.too_many_val,
                        },
              'nonum': {'val': 'burple',
                        'msg': hx.msg.invalid_time_mag_S % 'burple',
                        },
              'alphirst': {'val': 'ab234',
                           'msg': hx.msg.invalid_time_mag_S % 'ab234',
                           },
              'badunit': {'val': '9 yurt',
                          'msg': hx.msg.invalid_time_unit_S % 'yurt',
                          },
              'minus': {'val': '17-8',
                        'msg': hx.msg.too_many_val,
                        },
              }
        obj = hx.cfg.config.dictor({'crawler': {}, })
        for k in td:
            obj.set('crawler', k, td[k]['val'])

        for k in obj.options('crawler'):
            v = obj.get('crawler', k)
            self.assertRaisesMsg(ValueError,
                                 td[k]['msg'],
                                 obj.get_time,
                                 'crawler',
                                 k)

    # -------------------------------------------------------------------------
    def test_get_time_opt_def(self):
        """
        Call get_time so it throws NoOptionError but provide a default
        """
        self.dbgfunc()
        obj = hx.cfg.config.dictor(self.sample)
        self.expected(388, obj.get_time('crawler', 'dumpling', 388))
        self.expected(47, obj.get_time('crawler', 'strawberry', 47))
        self.expected(17.324, obj.get_time('crawler', 'beeswax', 17.324))
        self.assertRaisesMsg(U.HXerror,
                             hx.msg.default_int_float,
                             obj.get_time,
                             'crawler',
                             'fiddle',
                             'foobar')

    # -------------------------------------------------------------------------
    def test_get_time_sect_def(self):
        """
        Call get_time so it throws NoSectionError but provide a default
        """
        self.dbgfunc()
        obj = hx.cfg.config.dictor(self.sample)
        self.expected(82, obj.get_time('crawlerfoo', 'heartbeat', 82))
        self.expected(19, obj.get_time('crawlerfoo', 'frequency', 19))
        self.expected(17.324, obj.get_time('crawlerfoo', 'beeswax', 17.324))
        self.assertRaisesMsg(U.HXerror,
                             hx.msg.default_int_float,
                             obj.get_time,
                             'crawlerfoo',
                             'fiddle',
                             'foobar')

    # -------------------------------------------------------------------------
    def test_get_time_opt(self):
        """
        Call get_time() in such a way that it throws a NoOptionError so we can
        see that it adds the filename to the error message
        """
        section = U.my_name()
        obj = hx.cfg.config()
        obj.add_section(section)
        self.assertRaisesMsg(hx.cfg.NoOptionError,
                             "No option 'foobar' in section: '%s' in <???>" %
                             section,
                             obj.get_time,
                             section,
                             "foobar")

    # -------------------------------------------------------------------------
    def test_get_time_sect(self):
        """
        Call get_time() in such a way that it throws a NoSectionError so we can
        see that it adds the filename to the error message
        """
        section = U.my_name()
        obj = hx.cfg.config()
        self.assertRaisesMsg(hx.cfg.NoSectionError,
                             "No section: '%s' in <???>" %
                             section,
                             obj.get_time,
                             section,
                             "foobar")

    # -------------------------------------------------------------------------
    def test_getboolean(self):
        """
        Routines exercised: getboolean().
        """
        obj = hx.cfg.config()
        obj.add_section('abc')
        obj.set('abc', 'fire', 'True')
        obj.set('abc', 'other', 'False')
        obj.set('abc', 'bogus', "Santa Claus")
        self.expected(False, obj.getboolean('abc', 'flip'))
        self.expected(False, obj.getboolean('abc', 'other'))
        self.expected(True, obj.getboolean('abc', 'fire'))
        self.expected(False, obj.getboolean('abc', 'bogus'))
        self.expected(False, obj.getboolean('nosuch', 'fribble'))

    # -------------------------------------------------------------------------
    def test_interpolation_ok(self):
        """
        Test successful interpolation
        """
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = hx.cfg.config.dictor(d,
                                   defaults={'root':
                                             '/the/root/directory'})
        exp = "/the/root/directory/fiddle.log"
        actual = obj.get('crawler', 'logpath')
        self.expected(exp, actual)

    # -------------------------------------------------------------------------
    def test_interpolation_fail(self):
        """
        Failing interpolation should raise an exception
        """
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = hx.cfg.config.dictor(d,
                                   defaults={'xroot':
                                             '/there/is/no/root'})
        exp = "/the/root/directory/fiddle.log"
        self.assertRaisesMsg(hx.cfg.InterpolationMissingOptionError,
                             "Bad value substitution",
                             obj.get, 'crawler', 'logpath')

    # -------------------------------------------------------------------------
    def test_cc_log_0000(self):
        """
        CONDITIONS
        open:  A logger is already open before the test payload is called
        msg:   A message is passed to hx.cfg.log()
        close: In the call to hx.cfg.log(), close=True
        ohint: In the call to hx.cfg.log(), logpath and/or cfg are passed

        ACTIONS
        C - Close the open logger, if any
        N - Create a new logger
        W - write the log message

        The lines marked '!' are the cases most frequently used.

        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     0      0       0    ||                        test_cc_log_0000
           0     0      0       1    ||             n          test_cc_log_0001
           0     0      1       0    ||          c             test_cc_log_0010
           0     0      1       1    ||          c  n          test_cc_log_0011
           0     1      0       0  ! ||             n  w       test_cc_log_0100
           0     1      0       1    ||             n  w       test_cc_log_0101
           0     1      1       0    ||          c  n  w       test_cc_log_0110
           0     1      1       1    ||          c  n  w       test_cc_log_0111

           1     0      0       0    ||                        test_cc_log_1000
           1     0      0       1    ||                        test_cc_log_1001
           1     0      1       0  ! ||          c             test_cc_log_1010
           1     0      1       1  ! ||          c  n          test_cc_log_1011
           1     1      0       0  ! ||                w       test_cc_log_1100
           1     1      0       1    ||                w       test_cc_log_1101
           1     1      1       0    ||          c  n  w       test_cc_log_1110
           1     1      1       1    ||          c  n  w       test_cc_log_1111
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)

            # test payload
            l = hx.cfg.log()

            # test verification
            self.validate_logger(None, l)

    # -------------------------------------------------------------------------
    def test_cc_log_0001(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     0      0       1    ||             n
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)

            # test payload
            l = hx.cfg.log(logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_0010(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     0      1       0    ||          c
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)

            # test payload
            l = hx.cfg.log(close=True)

            # test verification
            self.validate_logger(None, l)

    # -------------------------------------------------------------------------
    def test_cc_log_0011(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     0      1       1    ||          c  n
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)
            exp_logpath = self.tmpdir(U.my_name())

            # test payload
            l = hx.cfg.log(close=True, logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_0100(self):
        """
        The '+ 4' added to f_lineno just before logging the message is the
        number of lines from where f_lineno reference is to where the log call
        actually happens. If they move relative to each other, this value will
        have to be updated.

        Case:
         open   msg   close   ohint  || action:  C  N  W
           0     1      0       0  ! ||             n  w
        """
        def escpar(q):
            if q.group(0) == '(':
                return r'\('
            elif q.group(0) == ')':
                return r'\)'
            else:
                return ''

        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            for filename in ["crawl.cfg", U.default_logpath()]:
                if os.path.exists(filename):
                    os.unlink(filename)

            # reset any config and logger already initialized
            hx.cfg.get_config(reset=True, soft=True)
            hx.cfg.log(close=True)

            # now compose message for logging
            msg = "This is a test log message %s"
            arg = "with a format specifier"
            exp_logfile = U.default_logpath()
            loc = "(%s:%d): " % (sys._getframe().f_code.co_filename,
                                 sys._getframe().f_lineno + 4)
            exp = (U.my_name() + loc + msg % arg)

            # test payload
            l = hx.cfg.log(msg, arg)

            # test verification
            self.validate_logger(exp_logfile,
                                 l,
                                 msg=re.sub("[)(]", escpar, exp))

    # -------------------------------------------------------------------------
    def test_cc_log_0101(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     1      0       1    ||             n  w
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)

            exp_msg = U.rstring()
            exp_logpath = self.tmpdir(U.my_name())

            # test payload
            l = hx.cfg.log(exp_msg, logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l, msg=exp_msg)

    # -------------------------------------------------------------------------
    def test_cc_log_0110(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     1      1       0    ||          c  n  w
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)
            d = copy.deepcopy(self.cdict)
            d['crawler']['logpath'] = self.logpath()
            self.write_cfg_file(self.default_cfname, d)
            exp_msg = U.rstring()

            # test payload
            l = hx.cfg.log(exp_msg, close=True)

            # test verification
            self.validate_logger(self.logpath(), l)

    # -------------------------------------------------------------------------
    def test_cc_log_0111(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           0     1      1       1    ||          c  n  w
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True)
            self.expected(None, l)
            exp_msg = U.rstring()
            exp_logpath = self.tmpdir(U.my_name())

            # test payload
            l = hx.cfg.log(exp_msg, close=True, logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_1000(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     0      0       0    ||           no-op
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            exp_logpath = self.tmpdir(U.my_name())
            l = hx.cfg.log(close=True, logpath=exp_logpath)
            self.expected(exp_logpath, l.handlers[0].stream.name)

            # test payload
            l = hx.cfg.log()

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_1001(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     0      0       1    ||           no-op
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            exp_logpath = self.tmpdir(U.my_name())
            ign_logpath = self.logpath()
            U.conditional_rm(ign_logpath)

            l = hx.cfg.log(close=True, logpath=exp_logpath)
            self.validate_logger(exp_logpath, l)

            # test payload
            l = hx.cfg.log(logpath=ign_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)
            self.assertPathNotPresent(ign_logpath)

    # -------------------------------------------------------------------------
    def test_cc_log_1010(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     0      1       0    ||          c
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            exp_logpath = self.tmpdir(U.my_name())
            l = hx.cfg.log(close=True, logpath=exp_logpath)
            self.validate_logger(exp_logpath, l)

            # test payload
            l = hx.cfg.log(close=True)

            # test verification
            self.validate_logger(None, l)

    # -------------------------------------------------------------------------
    def test_cc_log_1011(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     0      1       1    ||          c  n
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            exp_logpath = self.tmpdir(U.my_name())
            l = hx.cfg.log(close=True, logpath=self.logpath())
            self.validate_logger(self.logpath(), l)

            # test payload
            l = hx.cfg.log(close=True, logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_1100(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     1      0       0    ||                w
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            exp_logpath = self.tmpdir(U.my_name())
            l = hx.cfg.log(close=True, logpath=exp_logpath)
            self.validate_logger(exp_logpath, l)
            exp_msg = U.rstring()

            # test payload
            l = hx.cfg.log(exp_msg)

            # test verification
            self.validate_logger(exp_logpath, l, msg=exp_msg)

    # -------------------------------------------------------------------------
    def test_cc_log_1101(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     1      0       1    ||                w

        Since we already have a logger open and no close argument, the logpath
        argument in the test payload should be ignored. The message should be
        written to the log file that is already open.
        """
        self.dbgfunc()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            U.conditional_rm(self.logpath())
            exp_logpath = self.tmpdir(U.my_name())
            l = hx.cfg.log(close=True, logpath=exp_logpath)
            self.validate_logger(exp_logpath, l)
            exp_msg = U.rstring()

            # test payload
            l = hx.cfg.log(exp_msg, logpath=self.logpath())

            # test verification
            self.validate_logger(exp_logpath, l, msg=exp_msg)
            self.assertPathNotPresent(self.logpath())

    # -------------------------------------------------------------------------
    def test_cc_log_1110(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     1      1       0    ||          c  n  w

        The test payload calls for the currently open logger to be closed and a
        new one to be opened to receive the message. Since we're not passing
        any hints (logpath or cfg), the newly opened logger will be determined
        by either $CRAWL_LOG or the default config. In this case, we arrange
        for $CRAWL_LOG to be set to a known value so we know which log file
        should receive the message.
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", exp_logpath)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True, logpath=self.logpath())
            self.validate_logger(self.logpath(), l)
            exp_msg = U.rstring()

            # test payload
            l = hx.cfg.log(exp_msg, close=True)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def test_cc_log_1111(self):
        """
        Cases:
         open   msg   close   ohint  || action:  C  N  W
           1     1      1       1    ||          c  n  w

        This is almost the same as test 1110 above except that this time we are
        specifying a hint (logpath), so that will control the name of the newly
        opened logger.
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv("CRAWL_CONF", None),
                        U.tmpenv("CRAWL_LOG", None)):
            hx.cfg.get_config(reset=True, soft=True)
            l = hx.cfg.log(close=True, logpath=self.logpath())
            self.validate_logger(self.logpath(), l)
            exp_msg = U.rstring()

            # test payload
            l = hx.cfg.log(exp_msg, close=True, logpath=exp_logpath)

            # test verification
            self.validate_logger(exp_logpath, l)

    # -------------------------------------------------------------------------
    def validate_logger(self, expval, logger, msg=''):
        """
        Validate a logger, generating an appropriate message if anything is
        wrong with it
        """
        if expval is None:
            self.expected(None, logger)
            if msg != '':
                self.fail('TestError: No msg allowed when expval is None')
        else:
            self.expected(1, len(logger.handlers))
            self.expected(expval, logger.handlers[0].stream.name)
            self.assertPathPresent(expval)
            text = U.contents(expval)
            self.expected_in('-' * 15, text)
            if msg != '':
                self.expected_in(msg, text)

    # -------------------------------------------------------------------------
    def test_log_rollover_archive(self):
        """
        Create a logger with a low rollover threshold and an archive dir. Write
        to it until it rolls over. Verify that the archive file was handled
        correctly.
        """
        logbase = '%s.log' % U.my_name()
        logpath = self.tmpdir(logbase)
        logpath_1 = logpath + ".1"
        archdir = self.tmpdir("history")
        ym = time.strftime("%Y.%m%d")
        archlogpath = '%s/%s.%s-%s' % (archdir, logbase, ym, ym)
        lcfg_d = {'crawler': {'logpath': logpath,
                              'logsize': '500',
                              'archive_dir': archdir,
                              'logmax': '10'}}
        lcfg = hx.cfg.config.dictor(lcfg_d)
        hx.cfg.log(cfg=lcfg, close=True)
        lmsg = "This is a test " + "-" * 35
        for x in range(0, 5):
            hx.cfg.log(lmsg)

        self.assertTrue(os.path.isdir(archdir),
                        "Expected directory %s to be created" % archdir)
        self.assertTrue(os.path.isfile(logpath),
                        "Expected file %s to exist" % logpath)
        self.assertTrue(os.path.isfile(archlogpath),
                        "Expected file %s to exist" % archlogpath)

    # -------------------------------------------------------------------------
    def test_log_rollover_cwd(self):
        """
        Create a logger with a low rollover threshold and no archive dir. Write
        to it until it rolls over. Verify that the archive file was handled
        correctly.
        """
        logbase = '%s.log' % U.my_name()
        logpath = self.tmpdir(logbase)
        logpath_1 = logpath + ".1"
        ym = time.strftime("%Y.%m%d")
        archlogpath = self.tmpdir('%s.%s-%s' % (logbase, ym, ym))
        lcfg_d = {'crawler': {'logpath': logpath,
                              'logsize': '500',
                              'logmax': '10'}}
        lcfg = hx.cfg.config.dictor(lcfg_d)
        hx.cfg.log(cfg=lcfg, close=True)
        lmsg = "This is a test " + "-" * 35
        for x in range(0, 5):
            hx.cfg.log(lmsg)

        self.assertTrue(os.path.isfile(logpath),
                        "Expected file %s to exist" % logpath)
        self.assertFalse(os.path.isfile(archlogpath),
                         "Expected file %s to not exist" % archlogpath)

    # -------------------------------------------------------------------------
    def test_log_multfmt(self):
        """
        Test a log message with multiple formatters
        """
        fpath = self.tmpdir("%s.log" % U.my_name())
        hx.cfg.log(close=True)
        log = hx.cfg.log(logpath=fpath)

        # multiple % formatters in first arg
        a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f"
        a2 = "zebedee"
        a3 = 94
        a4 = 23.12348293402
        exp = (U.my_name() +
               "(%s:%d): " % (U.filename(), U.lineno()+2) +
               a1 % (a2, a3, a4))
        hx.cfg.log(a1, a2, a3, a4)
        result = U.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, U.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_onefmt(self):
        """
        Test a log message with just a single formatter
        """
        self.dbgfunc()
        fpath = self.tmpdir("%s.log" % U.my_name())
        hx.cfg.log(logpath=fpath, close=True)

        # 1 % formatter in first arg
        a1 = "This has a formatter and one argument: %s"
        a2 = "did that work?"
        exp = (U.my_name() +
               "(%s:%d): " % (U.filename(), U.lineno()+2) +
               a1 % a2)
        hx.cfg.log(a1, a2)
        result = U.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, U.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_000(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath')
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.logpath()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            hx.cfg.get_config(reset=True, soft=True)
            d = copy.deepcopy(self.cdict)
            d['crawler']['logpath'] = self.logpath()
            self.write_cfg_file(self.default_cfname, d)
            x = hx.cfg.new_logger()
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_001(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath')
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None)):
            U.conditional_rm(self.logpath())
            self.assertPathNotPresent(self.logpath())

            hx.cfg.get_config(reset=True, soft=True)
            self.write_cfg_file(self.default_cfname, self.cdict)

            d = copy.deepcopy(self.cdict)
            d['crawler']['logpath'] = exp_logpath
            xcfg = hx.cfg.config.dictor(d)

            x = hx.cfg.new_logger(cfg=xcfg)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_001a(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath')
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler

        In case 001, if cfg.get('crawler', 'logpath') is not set, use the
        default
        """
        self.dbgfunc()
        exp_logpath = U.default_logpath()
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            hx.cfg.get_config(reset=True, soft=True)
            self.write_cfg_file(self.default_cfname, self.cdict)

            d = copy.deepcopy(self.cdict)
            xcfg = hx.cfg.config.dictor(d)

            x = hx.cfg.new_logger(cfg=xcfg)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_010(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_LOG', exp_logpath)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            hx.cfg.get_config(reset=True, soft=True)
            self.write_cfg_file(self.default_cfname, self.cdict)

            x = hx.cfg.new_logger()
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_011(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_LOG', exp_logpath)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            hx.cfg.get_config(reset=True, soft=True)
            self.write_cfg_file(self.default_cfname, self.cdict)

            d = copy.deepcopy(self.cdict)
            d['crawler']['logpath'] = exp_logpath
            xcfg = hx.cfg.config.dictor(d)

            x = hx.cfg.new_logger(cfg=xcfg)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_100(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None),
                        U.tmpenv('CRAWL_LOG', None)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            x = hx.cfg.new_logger(logpath=exp_logpath)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_101(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None),
                        U.tmpenv('CRAWL_LOG', None)):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            xcfg = hx.cfg.config.dictor(self.cdict)

            x = hx.cfg.new_logger(logpath=exp_logpath, cfg=xcfg)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_110(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None),
                        U.tmpenv('CRAWL_LOG', self.logpath())):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            x = hx.cfg.new_logger(logpath=exp_logpath)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_cc_new_logger_111(self):
        """
        Routine new_logger() can be called eight different ways:
        logpath   env    cfg   || action
           0       0      0    ||  use the default ('crawl.cfg')
           0       0      1    ||  use cfg.get('crawler', 'logpath') or default
           0       1      0    ||  use os.getenv('CRAWL_CONF')
           0       1      1    ||  use os.getenv('CRAWL_CONF')
           1       0      0    ||  use logpath
           1       0      1    ||  use logpath
           1       1      0    ||  use logpath
           1       1      1    ||  use logpath

        The returned logger should never have more than one handler
        """
        self.dbgfunc()
        exp_logpath = self.tmpdir(U.my_name())
        with ctx.nested(U.Chdir(self.tmpdir()),
                        U.tmpenv('CRAWL_CONF', None),
                        U.tmpenv('CRAWL_LOG', self.logpath())):
            U.conditional_rm(exp_logpath)
            self.assertPathNotPresent(exp_logpath)

            xcfg = hx.cfg.config.dictor(self.cdict)

            x = hx.cfg.new_logger(logpath=exp_logpath, cfg=xcfg)
            self.expected(1, len(x.handlers))
            self.expected(exp_logpath, x.handlers[0].stream.name)
            self.assertPathPresent(exp_logpath)
            self.expected_in('-' * 15, U.contents(exp_logpath))

    # -------------------------------------------------------------------------
    def test_log_simple(self):
        """
        Tests for routine hx.cfg.log():
         - simple string in first argument
         - 1 % formatter in first arg
         - multiple % formatters in first arg
         - too many % formatters for args
         - too many args for % formatters
        """
        fpath = self.tmpdir("%s.log" % U.my_name())
        hx.cfg.log(logpath=fpath, close=True)

        # simple string in first arg
        exp = U.my_name() + ": " + "This is a simple string"
        hx.cfg.log(exp)
        result = U.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, U.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_toomany_fmt(self):
        """
        Too many formatters for the arguments should raise an exception
        """
        fpath = self.tmpdir("%s.log" % U.my_name())
        log = hx.cfg.log(logpath=fpath, close=True)

        # multiple % formatters in first arg

        argl = ["Here's a string: '%s'; here's an int: %d; " +
                "here's a float: %f; %g",
                "zebedee",
                94,
                23.12348293402]
        exp = U.my_name() + ": " + argl[0] % (argl[1],
                                              argl[2],
                                              argl[3],
                                              17.9)
        self.assertRaisesMsg(TypeError,
                             "not enough arguments for format string",
                             hx.cfg.log,
                             *argl)

        result = U.contents(fpath)
        self.assertFalse(exp in result,
                         "Not expecting '%s' in %s" %
                         (exp, U.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_toomany_args(self):
        """
        Too many arguments for the formatters should raise an exception
        """
        fpath = self.tmpdir("%s.log" % U.my_name())
        log = hx.cfg.log(logpath=fpath, close=True)

        # multiple % formatters in first arg
        argl = ["Here's a string: '%s'; here's an int: %d; here's a float: %f",
                "zebedee",
                94,
                23.12348293402,
                "friddle"]
        exp = (U.my_name() + ": " + argl[0] % (argl[1], argl[2], argl[3]))
        exc = "not all arguments converted during string formatting"
        self.assertRaisesMsg(TypeError,
                             exc,
                             hx.cfg.log,
                             *argl)

        result = U.contents(fpath)
        self.assertFalse(exp in result,
                         "Expected '%s' in %s" %
                         (exp, U.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_map_time_unit(self):
        """
        Routines exercised: __init__(), map_time_unit().
        """
        obj = hx.cfg.config()
        self.expected(1, obj.map_time_unit(''))
        self.expected(1, obj.map_time_unit('s'))
        self.expected(1, obj.map_time_unit('sec'))
        self.expected(1, obj.map_time_unit('seconds'))
        self.expected(60, obj.map_time_unit('m'))
        self.expected(60, obj.map_time_unit('min'))
        self.expected(60, obj.map_time_unit('minute'))
        self.expected(60, obj.map_time_unit('minutes'))
        self.expected(3600, obj.map_time_unit('h'))
        self.expected(3600, obj.map_time_unit('hr'))
        self.expected(3600, obj.map_time_unit('hour'))
        self.expected(3600, obj.map_time_unit('hours'))
        self.expected(24*3600, obj.map_time_unit('d'))
        self.expected(24*3600, obj.map_time_unit('day'))
        self.expected(24*3600, obj.map_time_unit('days'))
        self.expected(7*24*3600, obj.map_time_unit('w'))
        self.expected(7*24*3600, obj.map_time_unit('week'))
        self.expected(7*24*3600, obj.map_time_unit('weeks'))
        self.expected(30*24*3600, obj.map_time_unit('month'))
        self.expected(30*24*3600, obj.map_time_unit('months'))
        self.expected(365*24*3600, obj.map_time_unit('y'))
        self.expected(365*24*3600, obj.map_time_unit('year'))
        self.expected(365*24*3600, obj.map_time_unit('years'))

    # --------------------------------------------------------------------------
    def test_quiet_time_bound_mt(self):
        """
        Test a quiet time spec. Empty boundary -- hi == lo
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:17-19:17"
        cfg = hx.cfg.config.dictor(ldict)

        # front of day
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:01")

        # interval trailing edge
        self.try_qt_spec(cfg, False, "2014.0101 19:16:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:17:00")
        self.try_qt_spec(cfg, False, "2014.0101 19:17:01")

        # end of day
        self.try_qt_spec(cfg, False, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_bound_rsu(self):
        """
        Test a quiet time spec. Right side up (rsu) boundary -- lo < hi
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00"
        cfg = hx.cfg.config.dictor(ldict)

        # non-interval time
        self.try_qt_spec(cfg, False, "2014.0101 11:19:58")

        # interval leading edge
        self.try_qt_spec(cfg, False, "2014.0101 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0101 14:00:01")

        # interval time
        self.try_qt_spec(cfg, True, "2014.0101 15:28:19")

        # interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0101 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0101 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_bound_usd(self):
        """
        Test a quiet time spec. Upside-down (usd) boundary -- hi < lo
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:00-03:00"
        cfg = hx.cfg.config.dictor(ldict)

        # front of day
        self.try_qt_spec(cfg, True, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # mid quiet interval
        self.try_qt_spec(cfg, True, "2014.0101 02:19:32")

        # interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0101 02:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 03:00:00")
        self.try_qt_spec(cfg, False, "2014.0101 03:00:01")

        # mid non-interval
        self.try_qt_spec(cfg, False, "2014.0101 12:17:09")

        # interval leading edge
        self.try_qt_spec(cfg, False, "2014.0101 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:01")

        # mid quiet interval
        self.try_qt_spec(cfg, True, "2014.0101 21:32:19")

        # end of day
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_d_w(self):
        """
        Test a multiple quiet time spec. Date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2013.0428,fri"
        cfg = hx.cfg.config.dictor(ldict)

        # leading edge of date
        self.try_qt_spec(cfg, False, "2013.0427 23:59:59")
        self.try_qt_spec(cfg, True, "2013.0428 00:00:00")
        self.try_qt_spec(cfg, True, "2013.0428 00:00:01")

        # inside date
        self.try_qt_spec(cfg, True, "2013.0428 08:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2013.0428 23:59:59")
        self.try_qt_spec(cfg, False, "2013.0429 00:00:00")
        self.try_qt_spec(cfg, False, "2013.0429 00:00:01")

        # outside date, outside weekday
        self.try_qt_spec(cfg, False, "2013.0501 04:17:49")

        # leading edge of weekday
        self.try_qt_spec(cfg, False, "2013.0502 23:59:59")
        self.try_qt_spec(cfg, True, "2013.0503 00:00:00")
        self.try_qt_spec(cfg, True, "2013.0503 00:00:01")

        # inside weekday
        self.try_qt_spec(cfg, True, "2013.0503 11:23:01")

        # trailing edge of weekday
        self.try_qt_spec(cfg, True, "2013.0503 23:59:59")
        self.try_qt_spec(cfg, False, "2013.0504 00:00:00")
        self.try_qt_spec(cfg, False, "2013.0504 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_date(self):
        """
        Test a date quiet time spec. The edges are inclusive.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2014.0401"
        cfg = hx.cfg.config.dictor(ldict)

        # before date
        self.try_qt_spec(cfg, False, "2014.0331 23:00:00")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # inside date
        self.try_qt_spec(cfg, True, "2014.0401 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 14:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_missing(self):
        """
        When the config item is missing, quiet_time() should always return
        False
        """
        cfg = hx.cfg.config.dictor(self.cdict)

        # before date
        self.try_qt_spec(cfg, False, "2014.0331 23:00:00")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:01")

        # inside date
        self.try_qt_spec(cfg, False, "2014.0401 13:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 14:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 14:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, False, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_d(self):
        """
        Test a multiple quiet time spec. RSU boundary plus date.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2014.0401, 17:00 - 23:00"
        cfg = hx.cfg.config.dictor(ldict)

        # day before, before interval
        self.try_qt_spec(cfg, False, "2014.0331 03:07:18")

        # day before, leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0331 16:59:59")
        self.try_qt_spec(cfg, True, "2014.0331 17:00:00")
        self.try_qt_spec(cfg, True, "2014.0331 17:00:01")

        # day before, trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0331 22:59:59")
        self.try_qt_spec(cfg, True, "2014.0331 23:00:00")
        self.try_qt_spec(cfg, False, "2014.0331 23:00:01")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # inside date, before interval
        self.try_qt_spec(cfg, True, "2014.0401 16:19:11")

        # inside date, inside interval
        self.try_qt_spec(cfg, True, "2014.0401 18:19:11")

        # inside date, after interval
        self.try_qt_spec(cfg, True, "2014.0401 23:17:11")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

        # day after, before interval
        self.try_qt_spec(cfg, False, "2014.0402 16:19:11")

        # day after, leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0402 16:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 17:00:00")
        self.try_qt_spec(cfg, True, "2014.0402 17:00:01")

        # day after, inside interval
        self.try_qt_spec(cfg, True, "2014.0402 22:58:01")

        # day after, trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0402 22:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 23:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 23:00:01")

        # day after, after interval
        self.try_qt_spec(cfg, False, "2014.0402 23:19:20")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_d_w(self):
        """
        Test a multiple quiet time spec. rsu boundary plus date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,2012.0117,Wednes"
        cfg = hx.cfg.config.dictor(ldict)

        # before any of them, on Monday
        self.try_qt_spec(cfg, False, "2012.0116 11:38:02")

        # leading edge of the interval on Monday
        self.try_qt_spec(cfg, False, "2012.0116 13:59:59")
        self.try_qt_spec(cfg, True, "2012.0116 14:00:00")
        self.try_qt_spec(cfg, True, "2012.0116 14:00:01")

        # trailing edge of the interval on Monday
        self.try_qt_spec(cfg, True, "2012.0116 18:59:59")
        self.try_qt_spec(cfg, True, "2012.0116 19:00:00")
        self.try_qt_spec(cfg, False, "2012.0116 19:00:01")

        # leading edge of Tuesday, the 17th
        self.try_qt_spec(cfg, False, "2012.0116 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0117 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0117 00:00:01")

        # lunchtime on Tuesday
        self.try_qt_spec(cfg, True, "2012.0117 12:00:00")

        # interval on Tuesday
        self.try_qt_spec(cfg, True, "2012.0117 15:00:00")

        # trailing edge of Tuesday, leading edge of Wednesday
        self.try_qt_spec(cfg, True, "2012.0117 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0118 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0118 00:00:01")

        # lunchtime on Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 12:00:00")

        # interval on Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 15:00:00")

        # trailing edge of Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 23:59:59")
        self.try_qt_spec(cfg, False, "2012.0119 00:00:00")
        self.try_qt_spec(cfg, False, "2012.0119 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_w(self):
        """
        Test a multiple quiet time spec. RSU boundary plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,sat,Wednes"
        cfg = hx.cfg.config.dictor(ldict)

        # 2014.0301 is a saturday -- all times quiet
        self.try_qt_spec(cfg, True, "2014.0301 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:01")

        # 2014.0305 is a wednesday -- all times quiet
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:01")

        # 2014.0330 is a sunday -- leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0330 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0330 14:00:01")

        # 2014.0330 is a sunday -- trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0330 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0330 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_d(self):
        """
        Test a multiple quiet time spec. USD boundary plus date.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:00-8:15,2015.0217"
        cfg = hx.cfg.config.dictor(ldict)

        # in the early interval the day before
        self.try_qt_spec(cfg, True, "2015.0216 08:00:00")

        # trailing edge of the early interval the day before
        self.try_qt_spec(cfg, True, "2015.0216 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0216 08:15:00")
        self.try_qt_spec(cfg, False, "2015.0216 08:15:01")

        # outside the intervals the day before
        self.try_qt_spec(cfg, False, "2015.0216 18:30:00")

        # leading edge of late interval the day before
        self.try_qt_spec(cfg, False, "2015.0216 18:59:59")
        self.try_qt_spec(cfg, True, "2015.0216 19:00:00")
        self.try_qt_spec(cfg, True, "2015.0216 19:00:01")

        # leading edge of the date
        self.try_qt_spec(cfg, True, "2015.0216 23:59:59")
        self.try_qt_spec(cfg, True, "2015.0217 00:00:00")
        self.try_qt_spec(cfg, True, "2015.0217 00:00:01")

        # trailing edge of the early interval in the date
        self.try_qt_spec(cfg, True, "2015.0217 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0217 08:15:00")
        self.try_qt_spec(cfg, True, "2015.0217 08:15:01")

        # outside interval, in date
        self.try_qt_spec(cfg, True, "2015.0217 12:13:58")

        # leading edge of late interval in the date
        self.try_qt_spec(cfg, True, "2015.0217 18:59:59")
        self.try_qt_spec(cfg, True, "2015.0217 19:00:00")
        self.try_qt_spec(cfg, True, "2015.0217 19:00:01")

        # trailing edge of the date
        self.try_qt_spec(cfg, True, "2015.0217 23:59:59")
        self.try_qt_spec(cfg, True, "2015.0218 00:00:00")
        self.try_qt_spec(cfg, True, "2015.0218 00:00:01")

        # trailing edge of early interval the day after
        self.try_qt_spec(cfg, True, "2015.0218 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0218 08:15:00")
        self.try_qt_spec(cfg, False, "2015.0218 08:15:01")

        # after early interval day after
        self.try_qt_spec(cfg, False, "2015.0218 11:12:13")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_d_w(self):
        """
        Test a multiple quiet time spec. usd boundary plus date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,sat"
        cfg = hx.cfg.config.dictor(ldict)

        # 2014.0301 is a saturday
        self.try_qt_spec(cfg, True, "2014.0301 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:01")

        # saturday trailing edge
        self.try_qt_spec(cfg, True, "2014.0301 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0302 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0302 00:00:01")

        # 2014.0305 is a wednesday -- interval leading edge
        self.try_qt_spec(cfg, False, "2014.0305 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 14:00:01")

        # 2014.0305 is a wednesday -- interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0305 19:00:01")

        # 2014.0330 is a sunday -- interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0330 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0330 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_w(self):
        """
        Test a multiple quiet time spec. USD boundary plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "sat, sunday, 20:17 -06:45"
        cfg = hx.cfg.config.dictor(ldict)

        # Friday before 20:17
        self.try_qt_spec(cfg, False, "2012.0224 20:00:05")

        # friday at 20:17
        self.try_qt_spec(cfg, False, "2012.0224 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0224 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0224 20:17:01")

        # friday into saturday
        self.try_qt_spec(cfg, True, "2012.0224 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0225 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0225 00:00:01")

        # end of early interval saturday
        self.try_qt_spec(cfg, True, "2012.0225 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0225 06:45:00")
        self.try_qt_spec(cfg, True, "2012.0225 06:45:01")

        # during day saturday
        self.try_qt_spec(cfg, True, "2012.0225 13:25:01")

        # start of late interval saturday
        self.try_qt_spec(cfg, True, "2012.0225 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0225 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0225 20:17:01")

        # saturday into sunday
        self.try_qt_spec(cfg, True, "2012.0225 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0226 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0226 00:00:01")

        # end of early interval sunday
        self.try_qt_spec(cfg, True, "2012.0226 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0226 06:45:00")
        self.try_qt_spec(cfg, True, "2012.0226 06:45:01")

        # during day sunday
        self.try_qt_spec(cfg, True, "2012.0226 17:28:13")

        # start of late interval sunday
        self.try_qt_spec(cfg, True, "2012.0226 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0226 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0226 20:17:01")

        # sunday into monday
        self.try_qt_spec(cfg, True, "2012.0226 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0227 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0227 00:00:01")

        # end of early interval monday
        self.try_qt_spec(cfg, True, "2012.0227 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0227 06:45:00")
        self.try_qt_spec(cfg, False, "2012.0227 06:45:01")

        # during day monday
        self.try_qt_spec(cfg, False, "2012.0227 20:00:19")

    # --------------------------------------------------------------------------
    def test_quiet_time_wday(self):
        """
        Test a weekday. The edges are inclusive.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "Wednes"
        cfg = hx.cfg.config.dictor(ldict)

        # 2014.0305 is a wednesday -- beginning of day
        self.try_qt_spec(cfg, False, "2014.0304 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 00:00:01")

        # 2014.0305 is a wednesday -- inside day
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:01")

        # 2014.0305 is a wednesday -- end of day
        self.try_qt_spec(cfg, True, "2014.0305 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0306 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0306 00:00:01")

    # --------------------------------------------------------------------------
    def test_read_include(self):
        """
        Read a config file which contains an include option
        """
        cfname_root = self.tmpdir("inclroot.cfg")
        cfname_inc1 = self.tmpdir("include1.cfg")
        cfname_inc2 = self.tmpdir("include2.cfg")

        root_d = copy.deepcopy(self.cdict)
        root_d['crawler']['include'] = cfname_inc1

        inc1_d = {'crawler': {'logmax': '17',
                              'coal': 'anthracite'
                              },
                  'newsect': {'newopt': 'newval',
                              'include': cfname_inc2}
                  }

        inc2_d = {'fiddle': {'bar': 'wumpus'}}

        self.write_cfg_file(cfname_root, root_d)
        self.write_cfg_file(cfname_inc1, inc1_d)
        self.write_cfg_file(cfname_inc2, inc2_d, includee=True)

        obj = hx.cfg.config()
        obj.read(cfname_root)

        root_d['crawler']['logmax'] = '17'
        root_d['crawler']['coal'] = 'anthracite'

        for D in [root_d, inc1_d, inc2_d]:
            for section in D:
                for option in D[section]:
                    self.expected(D[section][option], obj.get(section, option))

    # --------------------------------------------------------------------------
    def test_read_warn(self):
        """
        Read a config file which contains an include option. If some included
        files don't exist, we should get a warning that they were not loaded.
        """
        cfname_root = self.tmpdir("inclroot.cfg")
        cfname_inc1 = self.tmpdir("include1.cfg")
        cfname_inc2 = self.tmpdir("include2.cfg")

        root_d = copy.deepcopy(self.cdict)
        root_d['crawler']['include'] = cfname_inc1

        inc1_d = {'crawler': {'logmax': '17',
                              'coal': 'anthracite'
                              },
                  'newsect': {'newopt': 'newval',
                              'include': cfname_inc2
                              }
                  }

        self.write_cfg_file(cfname_root, root_d)
        self.write_cfg_file(cfname_inc1, inc1_d)

        obj = hx.cfg.config()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # this read should raise a warning about some config file(s) not
            # being loaded
            obj.read(cfname_root)
            # we only should have gotten one warning
            self.expected(1, len(w))
            # it should be a UserWarning
            self.assertTrue(issubclass(w[-1].category, UserWarning),
                            "Expected a UserWarning, but got a %s" %
                            w[-1].category)
            # it should contain this string
            self.assertTrue("Some config files not loaded" in str(w[-1]),
                            "Unexpected message: '%s'" % str(w[-1]))

        root_d['crawler']['logmax'] = '17'
        root_d['crawler']['coal'] = 'anthracite'

        for D in [root_d, inc1_d]:
            for section in D:
                for option in D[section]:
                    self.expected(D[section][option], obj.get(section, option))

    # --------------------------------------------------------------------------
    def test_root_absolute(self):
        """
        Verify that root gets absolutized when the hx.cfg is initialized
        """
        new = hx.cfg.config({'root': '.'})
        self.expected(os.getcwd(), new.get('DEFAULT', 'root'))
        new.add_section('crawler')
        new.set('crawler', 'logpath', '%(root)s/xyzzy.log')
        self.expected("%s/xyzzy.log" % os.getcwd(),
                      new.get('crawler', 'logpath'))

    # ------------------------------------------------------------------------
    def try_qt_spec(self, cfg, exp, tval):
        """
        Single quiet_time test.
        """
        try:
            qtspec = cfg.get('crawler', 'quiet_time')
        except hx.cfg.NoOptionError:
            qtspec = "<empty>"

        actual = cfg.quiet_time(U.epoch(tval))
        self.assertEqual(exp, actual,
                         "'%s'/%s => expected '%s', got '%s'" %
                         (qtspec,
                          tval,
                          exp,
                          actual))
