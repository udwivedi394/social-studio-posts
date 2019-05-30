import random
import time
from datetime import datetime
from typing import Dict
from urllib.parse import urlsplit

import requests
from fake_useragent import UserAgent
from requests import Response


class RequestsTimeout:
    CONNECTION_TIMEOUT = 300
    READ_TIMEOUT = 300

    TIMEOUT_TUPLE = (CONNECTION_TIMEOUT, READ_TIMEOUT)


class Throttle:
    def __init__(self, delay=60):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """ Delay if have accessed this domain recently"""
        domain = urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            print("Sleeping for: ", sleep_secs)
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()


class WebScraperUtility:
    def __init__(self, delay=5):
        self.headers = []
        self.throttle = Throttle(delay=delay)

    def get_useragent(self):
        ua = UserAgent()
        self.headers = [ua.chrome, ua.google,
                        ua['google chrome'], ua.firefox, ua.ff]
        return {'User-Agent': random.choice(self.headers)}

    def wait(self, url):
        """ Delay if have accessed this domain recently"""
        self.throttle.wait(url)


class RequestMaker:
    def __init__(self, header=None, delay=10):
        self.delay = delay
        self.scrape_utility = WebScraperUtility(delay=delay)
        self.proxy_dict = None
        self.extra_header = header if header else {}

    def _make_request(self, url, request_method, parameters, retry, header=None):
        i = 0
        base_header = self.scrape_utility.get_useragent()
        extra_header = header if header else self.extra_header
        headers = {**base_header, **extra_header}
        status_message = None
        request_parameters = {'proxies': self.proxy_dict,
                              'timeout': RequestsTimeout.TIMEOUT_TUPLE}
        request_parameters = {**parameters, **request_parameters}
        while i < retry:
            i += 1
            try:
                self.scrape_utility.wait(url)
                page = request_method(url, headers=headers, **request_parameters)
                status_message = 'Status: {status}, {reason} for URL: {url}'.format(status=page.status_code,
                                                                                    reason=page.reason, url=url)
                if page.status_code == 200:
                    return page
                elif page.status_code == 404:
                    raise ValueError(status_message)
                print(" Status code :{status}, Retrying ...".format(status=page.status_code))
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(self.delay)
                print("Retrying")
                print(url)
                status_message = 'RequestException {reason} for URL: {url}'.format(reason=e, url=url)
        raise ValueError(status_message)

    def get_request(self, url: str, params=None, retry=5, header=None) -> Response:
        request_method = requests.get
        request_parameters = {'params': params}
        return self._make_request(url, request_method, parameters=request_parameters, retry=retry, header=header)

    def post_request(self, url: str, json=None, retry=5, header=None) -> Response:
        request_method = requests.post
        request_parameters = {'data': json}
        return self._make_request(url, request_method, parameters=request_parameters, retry=retry, header=header)

    def activate_proxy(self, host, port, username, password) -> Dict:
        proxy = 'http://{}:{}@{}:{}'.format(username, password, host, port)
        self.proxy_dict = {'http': proxy,
                           'https': proxy}
        return self.proxy_dict
