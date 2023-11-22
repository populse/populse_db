import os
import unittest

from . import load_tests

# Working from the scripts directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

unittest.main()
