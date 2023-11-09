import os.path
import sys

# populse_db current version
version_major = 3
version_minor = 0
version_micro = 0
version_extra = "alpha"

# Expected by setup.py: string of form "X.Y.Z"
__version__ = "{0}.{1}.{2}".format(version_major, version_minor, version_micro)

# Expected by setup.py: the status of the project
CLASSIFIERS = ['Development Status :: 5 - Production/Stable',
               'Environment :: Console',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 3.9',
               'Programming Language :: Python :: 3.10',
               'Programming Language :: SQL',
               'Natural Language :: English',
               'Topic :: Database',
               ]

# Project descriptions
NAME = 'populse-db'
DESCRIPTION = 'populse-db'
LONG_DESCRIPTION = '''
==========
populse_db
==========

The meta-data storage and query system for Populse.
'''
# Populse project
PROJECT = 'populse'
brainvisa_build_model = 'pure_python'

# Other values used in setup.py
ORGANISATION = 'populse'
AUTHOR = 'Populse'
AUTHOR_EMAIL = 'yann@cointepas.net'
URL = 'http://populse.github.io/populse_db/'
LICENSE = 'CeCILL-B'
VERSION = __version__
PLATFORMS = 'OS Independent'
REQUIRES = [
    'python-dateutil',
    'lark-parser >=0.7.0'
]
EXTRA_REQUIRES = {
    'doc': [
        'sphinx >=1.0',
    ],
    'postgres': [
        'psycopg2-binary',
    ],
}

# tests to run
test_commands = [f'{os.path.basename(sys.executable)} -m populse_db.test']
