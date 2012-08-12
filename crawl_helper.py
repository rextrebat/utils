#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Helper for crawling"""

__author__ = "Kingshuk Dasgupta (rextrebat/kdasgupta)"
__version__ = "0.0pre0"

import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

import urllib2
import urllib
import json
import BeautifulSoup

import time


logger = logging.getLogger('crawl_helper')

# -- globals

class ResponseFormat:
    RAW=1
    STR=2
    JSON=3
    SOUP=4


class Throttler(object):
    """
    Throttle Manager:
    FIXME: Make this threadsafe
    """

    def __init__(self, limit_requests, 
            limit_interval=1, sleep_bw_checks=0.1):
        self.limit_requests = limit_requests
        self.limit_interval = limit_interval
        self.sleep_bw_checks = sleep_bw_checks
        self.requests = []

    def next_request(self):
        """
        This method will return when it is ok to make the next request
        """
        while True:
            now = time.time()
            t_begin = now - self.limit_interval
            self.requests = [r for r in self.requests if r > t_begin]
            if len(self.requests) >= self.limit_requests:
                time.sleep(self.sleep_bw_checks)
            else:
                self.requests.append(now)
                logger.debug("Throttler - request added %s" % (str(now)))
                break

class HTTPRequester(object):
    """
    Requester class: 
    FIXME: Add error handling
    """

    def __init__(
            self,
            base_url,
            base_params={},
            response_format=ResponseFormat.RAW,
            throttler=None,
            ):
        self.base_url = base_url
        self.base_params = base_params
        self.response_format = response_format
        self.throttler=throttler

    def get(self, params=None):
        if params or self.base_params:
            url = self.base_url + "?"
        else:
            url = self.base_url
        url += urllib.urlencode(self.base_params)
        if self.base_params:
            url += "&" + urllib.urlencode(params)
        if self.throttler:
            self.throttler.next_request()
        try:
            response = urllib2.urlopen(url)
            logger.debug("Response: \n %s" % (str(response)[:1024]))
            if self.response_format == ResponseFormat.RAW:
                return response
            elif self.response_format == ResponseFormat.STR:
                return str(response)
            elif self.response_format == ResponseFormat.JSON:
                return json.loads(response)
            elif self.response_format == ResponseFormat.SOUP:
                return BeautifulSoup(response)
        except urllib2.URLError, e:
            logger.debug("Failure %s" % (e.reason))
