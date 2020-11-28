#!/usr/bin/env python3

from setuptools import setup, find_namespace_packages
from mt.streamz.version import version

setup(name='mtstreamz',
      version=version,
      description="Minh-Tri Pham's extension of the streamz Python library",
      author=["Minh-Tri Pham"],
      packages=find_namespace_packages(include=['mt.*']),
      install_requires=[
          'tornado>=6.0.4', # 2020/08/27: tornado 6 is needed and distributed must be >= v2.24.0
          'dask>=2.24.0', # for simple multiprocessing jobs
          'distributed>=2.24.0',  # for simple multiprocessing jobs
          'streamz>=0.5.5', # for streaming
          'mtbase>=1.0.0',
      ],
      url='https://github.com/inteplus/mtstreamz',
      project_urls={
          'Documentation': 'https://mtdoc.readthedocs.io/en/latest/mt.streamz/mt.streamz.html',
          'Source Code': 'https://github.com/inteplus/mtstreamz',
          }
      )
