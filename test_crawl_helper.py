#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Helper for crawling"""

__author__ = "Kingshuk Dasgupta (rextrebat/kdasgupta)"
__version__ = "0.0pre0"

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

import unittest
import mock
import crawl_helper
import time


class TestRequester(unittest.TestCase):

    @mock.patch("crawl_helper.urllib2")
    def test_throttle1(self, mock_urllib2):
        throttler = crawl_helper.Throttler(3, 1)
        requester = crawl_helper.HTTPRequester(
                base_url="testurl",
                throttler=throttler
                )
        for i in range(100):
            requester.get()
            time.sleep(0.1)

    @mock.patch("crawl_helper.urllib2")
    def test_throttle2(self, mock_urllib2):
        throttler = crawl_helper.Throttler(5, 1)
        requester = crawl_helper.HTTPRequester(
                base_url="testurl",
                throttler=throttler
                )
        for i in range(100):
            requester.get()
            time.sleep(0.5)

    def process_response(self, s):
        print s

    @mock.patch("crawl_helper.urllib2")
    def test_fetch(self, mock_urllib2):
        config = crawl_helper.FetcherConfig(
                base_url="testurl")
        throttler1 = crawl_helper.Throttler(5, 1)
        fetcher_pool = crawl_helper.FetcherPool(
                size=3,
                throttlers=[throttler1])
        for i in range(100):
            fetch_task = crawl_helper.FetchTask(
                    config=config,
                    process_response=self.process_response
                    )
            fetcher_pool.queue.put(fetch_task)
        while not fetcher_pool.queue.empty():
            time.sleep(0.1)
        time.sleep(2)
        fetcher_pool.stop()


if __name__ == '__main__':
    unittest.main()
