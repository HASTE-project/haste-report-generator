#!/usr/bin/env python

from distutils.core import setup

setup(name='haste_report_generator',
      packages=['haste.report_generator'],
      install_requires=[
          'pymongo'
      ],
      )
