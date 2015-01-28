#!/usr/bin/env python

from setuptools import setup
import os

exec(open(os.path.join('hx', 'version.py')).read())

setup(name='hx',
      version=__version__,
      description='HPSS Python Toolbox',
      author='Tom Barron',
      author_email='tbarron@ornl.gov',
      url='https://github.com/ORNL-TechInt/hx',
      packages=['hx', 'tests'],
      )
