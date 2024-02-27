#!/usr/bin/env python3

import os
from setuptools import setup, find_namespace_packages

VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION.txt")

setup(
    name="mtstreamz",
    description="Minh-Tri Pham's extension of the streamz Python library",
    author=["Minh-Tri Pham"],
    packages=find_namespace_packages(include=["mt.*"]),
    install_requires=[
        "tornado>=6.0.4",  # 2020/08/27: tornado 6 is needed and distributed must be >= v2.24.0
        "dask>=2.24.0",  # for simple multiprocessing jobs
        "distributed>=2.24.0",  # for simple multiprocessing jobs
        "streamz>=0.5.5",  # for streaming
        "mtbase>=1.0.0",
    ],
    url="https://github.com/inteplus/mtstreamz",
    project_urls={
        "Documentation": "https://mtdoc.readthedocs.io/en/latest/mt.streamz/mt.streamz.html",
        "Source Code": "https://github.com/inteplus/mtstreamz",
    },
    setup_requires=["setuptools-git-versioning<2"],
    setuptools_git_versioning={
        "enabled": True,
        "version_file": VERSION_FILE,
        "count_commits_from_version_file": True,
        "template": "{tag}",
        "dev_template": "{tag}.dev{ccount}+{branch}",
        "dirty_template": "{tag}.post{ccount}",
    },
)
