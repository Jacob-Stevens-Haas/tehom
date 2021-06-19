# -*- coding: utf-8 -*-
from setuptools import setup

import versioneer



setup(
      cmdclass = versioneer.get_cmdclass(),
      version = versioneer.get_version()
)
