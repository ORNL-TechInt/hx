import os
import pdb
import pytest
import sys
import hx.version
import time


# -----------------------------------------------------------------------------
def hx_test_log(config, loggable):
    """
    Write to the default log path unless the user said --nolog
    """
    if config.getoption("nolog"):
        return
    msg = "%s %s\n" % (time.strftime("%Y.%m%d %H:%M:%S"),
                       loggable)
    with open(hx_test_log._logpath, 'a') as f:
        f.write(msg)


# -----------------------------------------------------------------------------
def pytest_addoption(parser):
    """
    Add options --nolog, --all to the command line
    """
    global attr
    parser.addoption("--all", action="store_true",
                     help="run all tests")
    parser.addoption("--dbg", action="append", default=[],
                     help="start debugger on named test or all")
    parser.addoption("--fast", action="store_true",
                     help="run only the fast tests")
    parser.addoption("--keep", action="store_true",
                     help="preserve test tables")
    parser.addoption("--nolog", action="store_true", default=False,
                     help="suppress test logging")
    parser.addoption("--skip", action="append", default=[],
                     help="skip tests with matching tags")
    attr = pytest.mark.attr
    if os.path.isdir('build'):
        raise SystemExit('\n   Directory build will cause test failures. ' +
                         'Please remove it and try again.\n')


# -----------------------------------------------------------------------------
def pytest_report_header(config):
    """
    Add version to header of test report
    """
    hx_test_log(config, "-" * 60)
    return("Testing hx version %s" % hx.version.__version__)


# -----------------------------------------------------------------------------
def pytest_runtest_setup(item):
    """
    Decide whether to skip a test before running it
    """
    if item.cls is None:
        fqn = '.'.join([item.module.__name__, item.name])
    else:
        fqn = '.'.join([item.module.__name__, item.cls.__name__, item.name])
    dbg_n = '..' + item.name
    dbg_l = item.config.getvalue('dbg')
    skip_l = item.config.getvalue('skip')
    if dbg_n in dbg_l or '..all' in dbg_l:
        pdb.set_trace()

    if any([item.name in item.config.getoption('--dbg'),
            'all' in item.config.getoption('--dbg')]):
        pytest.dbgfunc = pdb.set_trace
    else:
        pytest.dbgfunc = lambda: None

    jfm = 'jenkins_fail'
    if item.get_marker(jfm):
        if os.path.exists('jenkins') or jfm in skip_l:
            pytest.skip('%s would fail on jenkins' % fqn)

    slow_m = 'slow'
    if item.get_marker('slow'):
        if item.config.getvalue('fast') or slow_m in skip_l:
            pytest.skip('%s is slow' % fqn)

    for skiptag in item.config.getvalue('skip'):
        if skiptag in fqn:
            pytest.skip("Skiptag '%s' excludes '%s'" % (skiptag, fqn))


# -----------------------------------------------------------------------------
@pytest.mark.tryfirst
def pytest_runtest_makereport(item, call, __multicall__):
    """
    Write a line to the log file for this test
    """
    rep = __multicall__.execute()
    if rep.when != 'call':
        return rep

    if rep.outcome == 'failed':
        status = ">>>>FAIL"
        hx_test_log._failcount += 1
    elif rep.outcome == 'skipped':
        status = "**SKIP**"
        hx_test_log._skipcount += 1
    else:
        status = "--pass"
        hx_test_log._passcount += 1

    parent = item.parent
    msg = "%-8s %s:%s.%s" % (status,
                             os.path.basename(parent.fspath.strpath),
                             parent.name,
                             item.name)
    hx_test_log(item.config, msg)
    return rep


# -----------------------------------------------------------------------------
def pytest_unconfigure(config):
    """
    We're on our way out -- write the test summary to the log file
    """
    hx_test_log(config,
                    "passed: %d; skipped: %d; FAILED: %d" %
                    (hx_test_log._passcount,
                     hx_test_log._skipcount,
                     hx_test_log._failcount))


hx_test_log._logpath = "tests/hx_test.log"
hx_test_log._passcount = 0
hx_test_log._skipcount = 0
hx_test_log._failcount = 0
