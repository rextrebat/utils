#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Helper for crawling"""

__author__ = "Kingshuk Dasgupta (rextrebat/kdasgupta)"
__version__ = "0.0pre0"

import logging

import urllib2
import urllib
import json
import BeautifulSoup

import time
import threading
import Queue


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
    """

    def __init__(self, limit_requests, 
            limit_interval=1, sleep_bw_checks=0.1):
        self.limit_requests = limit_requests
        self.limit_interval = limit_interval
        self.sleep_bw_checks = sleep_bw_checks
        self.requests = []
        self.lock = threading.Lock()

    def next_request(self):
        """
        This method will return when it is ok to make the next request
        """
        with self.lock:
            while True:
                now = time.time()
                t_begin = now - self.limit_interval
                self.requests = [r for r in self.requests if r > t_begin]
                if len(self.requests) >= self.limit_requests:
                    logger.debug("Throttling...")
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
            headers=None,
            ):
        self.base_url = base_url
        self.base_params = base_params
        self.response_format = response_format
        self.throttler = throttler
        self.headers = headers

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
            logger.debug("Request: \n %s" % (str(url)[:1024]))
            response = urllib2.urlopen(url).read()
            logger.debug("Response: \n %s" % (str(response)[:1024]))
            if self.response_format == ResponseFormat.RAW:
                return response
            elif self.response_format == ResponseFormat.STR:
                return str(response)
            elif self.response_format == ResponseFormat.JSON:
                return json.loads(response)
            elif self.response_format == ResponseFormat.SOUP:
                return BeautifulSoup.BeautifulSoup(response)
        except urllib2.URLError as e:
            logger.error("Failure %s" % (e.reason))


class FetcherConfig:
    """
    Fetcher Config
    """

    def __init__(
            self,
            base_url,
            base_params={},
            response_format=ResponseFormat.RAW,
            headers={},
            ):
        self.base_url = base_url
        self.base_params = base_params
        self.response_format = response_format
        self.headers = headers


class FetchTask:
    """
    Fetch Task
    """ 
    
    def __init__(self, config, params=None, params_no_encode=None,
            context=None, process_response=None):
        self.config = config
        self.params = params
        self.params_no_encode = params_no_encode
        self.context = context
        self.process_response = process_response

    def respond(self, response):
        if self.config.response_format == ResponseFormat.RAW:
            pass
        elif self.config.response_format == ResponseFormat.STR:
            response = str(response)
        elif self.config.response_format == ResponseFormat.JSON:
            response = json.loads(response)
        elif self.config.response_format == ResponseFormat.SOUP:
            response = BeautifulSoup(response)
        if not self.context:
            return self.process_response(response)
        else:
            return self.process_response(response, self.context)



class Fetcher(threading.Thread):
    """
    Fetcher
    """

    def __init__(self, queue, throttlers=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.throttlers = throttlers
        self.terminate = False

    def fetch(self, task):
        if task.params or task.config.base_params or task.params_no_encode:
            url = task.config.base_url + "?"
        else:
            url = task.config.base_url
        param_added = False
        if task.config.base_params:
            url += urllib.urlencode(task.config.base_params)
            param_added = True
        if task.params:
            if param_added:
                url += "&"
            url += urllib.urlencode(task.params)
        if task.params_no_encode:
            if param_added:
                url += "&"
            url += task.params_no_encode
        try:
            logger.debug("Request: \n %s" % (str(url)[:1024]))
            request = urllib2.Request(url=url, headers=task.config.headers)
            response = urllib2.urlopen(request).read()
            logger.debug("Response: \n %s" % (str(response)[:1024]))
            return task.respond(response)
        except urllib2.URLError as e:
            logger.error("Failure %s" % (e.reason))

    def run(self):
        while not self.terminate:
            try:
                task = self.queue.get()
                if self.throttlers:
                    for throttler in self.throttlers:
                        throttler.next_request()
                self.fetch(task)
            except Queue.Empty:
                pass


class FetcherPool:
    """
    FetcherPool
    """

    def __init__(self, size, throttlers=None):
        self.size = size
        self.throttlers = throttlers
        self.queue = Queue.Queue()
        self.fetchers = [Fetcher(
            self.queue, self.throttlers) for i in range(size)]
        for fetcher in self.fetchers:
            fetcher.daemon = True
            fetcher.start()

    def stop(self):
        for fetcher in self.fetchers:
            fetcher.terminate = True
