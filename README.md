<head>
<title>HPSS Python Toolbox</title>
</head>

# HPSS Python Toolbox

This library provides code that both HPSS Integrity Crawler (hpssic)
and HPSS Disaster Recovery (hpssdr) use.

## Modules

### cfg

Provides a cfg class based on Python's ConfigParser class with the
following added functionality:

* provides a well-defined precedence for selecting an appropriate
  configuration

* update detection so changes config files can be reloaded without
  requiring user intervention

* log file management

* ability to provide default values so missing config items don't
  raise an exception

* size and time semantics for expressions like '10kb' and '5min'

* tolerance of missing config items through default values and a
  special get_boolean() method

* cfg object can be initialized from a dictionary

* quiet time handling

* support included config files

### dbi

Provides a method-based database interface with support for sqlite,
MySQL, and DB2.

### testhelp

Testing support.

* HelpedTestCase: a test case class with additional assertions,
  defaults, per-method debug hook, pytest tmpdir support for test classes

* assertion timeouts (i.e., if the test passes before the timeout
  expires, pass; otherwise, fail)

* transparent handling of "No data collected" warnings from coverage

### util

* context managers for temporary environment settings, temporary directory excursions

* a log file handler that archives log files when they get full

* a reverse read class for reading a file backward

* convenience routines for absolute path names, basename, dirname,
  environment variable and user path expansion

* case insensitive sort

* memoization

* various date/time computations

* assorted utility routines