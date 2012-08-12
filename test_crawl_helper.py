#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Helper for crawling"""

__author__ = "Kingshuk Dasgupta (rextrebat/kdasgupta)"
__version__ = "0.0pre0"

import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

import unittest
import mock
import crawl_helper
import time


class TestRequester(unittest.TestCase):

    @mock.patch("crawl_helper.urllib2")
    def test_throttle(self, mock_urllib2):
        throttler = crawl_helper.Throttler(3, 1)
        requester = crawl_helper.HTTPRequester(
                base_url="testurl",
                throttler=throttler
                )
        for i in range(100):
            requester.get()
            time.sleep(0.1)


if __name__ == '__main__':
    unittest.main()
