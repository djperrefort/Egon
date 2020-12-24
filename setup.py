#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from setuptools import find_packages, setup

import egon

# Get list of requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='egon',
      version=egon.__version__,
      packages=find_packages(),
      keywords='Data Pipeline Parallel Pipeline',
      description='A lightweight, minimalist framework for the rapid development of analysis pipelines.',
      long_description=egon.__doc__,
      long_description_content_type='text/markdown',
      classifiers=[
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python'],
      author='Daniel Perrefort',
      author_email='djperrefort@pitt.edu',
      license='GPL v3',
      python_requires='>=3.6',
      install_requires=requirements
      )
