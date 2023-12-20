# -*- coding: utf-8 -*-

"""
The module dedicated to the main information on populse_db.

The info.py module is mainly used by the setup.py module.
"""
##########################################################################
# Populse_db - Copyright (C) IRMaGe/CEA, 2018
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################

import os.path
import sys

# populse_db current version
version_major = 2
version_minor = 5
version_micro = 2
version_extra = ""

# Expected by setup.py: string of form "X.Y.Z"
__version__ = "{0}.{1}.{2}".format(version_major, version_minor, version_micro)

# Expected by setup.py: the status of the project
CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: CEA CNRS Inria "
    "Logiciel Libre License, version 2.1 (CeCILL-2.1)",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3 :: Only",
    "Topic :: Scientific/Engineering",
    "Topic :: Utilities",
]

# Project descriptions
NAME = "populse-db"
DESCRIPTION = "populse-db"
LONG_DESCRIPTION = """
==========
populse_db
==========

The meta-data storage and query system for Populse.
"""
# Populse project
PROJECT = "populse"
brainvisa_build_model = "pure_python"

# Other values used in setup.py
ORGANISATION = "populse"
AUTHOR = "Populse"
AUTHOR_EMAIL = "yann@cointepas.net"
URL = "http://populse.github.io/populse_db/"
LICENSE = "CeCILL-B"
VERSION = __version__
CLASSIFIERS = CLASSIFIERS
PLATFORMS = "OS Independent"
REQUIRES = ["python-dateutil", "lark"]
EXTRA_REQUIRES = {
    "doc": [
        "sphinx >=1.0",
    ],
    "postgres": [
        "psycopg2-binary",
    ],
}

# tests to run
test_commands = ["%s -m populse_db.test" % os.path.basename(sys.executable)]
