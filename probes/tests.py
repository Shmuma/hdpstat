from django.utils import unittest

from lib import test_sliding_averager

def suite ():
    return unittest.TestLoader().loadTestsFromModule(test_sliding_averager)
