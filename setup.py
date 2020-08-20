#!/usr/bin/env python

from setuptools import setup

setup(name='fs.anvilfs',
      version='0.0',
      packages=setuptools.find_packages(),
      install_requires=[
            'firecloud>=0.16.28',
            'google-cloud-storage',
            'fs'
      ],
      description='PyFilesystem2 AnVIL plugin')