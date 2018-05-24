#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import os
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from corpus.util.config import fetch_proxy
import asyncio
import concurrent.futures


class Headless:
    def __init__(self, loop=None,  use_proxy=False, pool_size=10, log_path='headless'):
        self.loop = loop if loop else asyncio.get_event_loop()

        self.use_proxy = use_proxy

        self.capab = DesiredCapabilities.CHROME
        self.capab['loggingPrefs'] = {'browser': 'ALL'}

        self.log_path=log_path

        self.driver_pool = asyncio.Queue(loop=self.loop)
        self.loop.run_until_complete(self.init_pool(pool_size))

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=pool_size)

    async def init_driver(self):
        driver_options = webdriver.ChromeOptions()
        driver_options.add_argument('headless')
        if self.use_proxy:
            proxy = await fetch_proxy()
            driver_options.add_argument('--proxy-server=%s' % proxy)
        return webdriver.Chrome(
                                chrome_options=driver_options,
                                service_log_path=self.log_path,
                                desired_capabilities=self.capab)

    async def init_pool(self, pool_size):
        for i in range(pool_size):
            driver = await self.init_driver()
            await self.driver_pool.put((driver, 0))

    async def clear_pool(self):
        while len(self.driver_pool) > 0:
            driver, count = await self.driver_pool.get()
            driver.quit()

    def close(self):
        self.loop.run_until_complete(self.clear_pool())

    async def get(self, url, locator=None, timeout=5):

        def _get(_driver, _url):
            wait = WebDriverWait(_driver, timeout, poll_frequency=1)
            try:
                _driver.delete_all_cookies()
                _driver.get(_url)
                if locator:
                    wait.until(EC.presence_of_all_elements_located(locator))
                else:
                    wait.until(lambda x: x.execute_script('return document.readyState;') == 'complete')
                return _driver.page_source
            except Exception as e:
                print(url, e)

            return None

        # get driver
        driver, count = await self.driver_pool.get()
        if count > 10:
            driver.quit()
            driver = await self.init_driver()
            count = 0

        # get html
        times = 10
        while times > 0:
            times -= 1
            html = await self.loop.run_in_executor(self.executor, _get, driver, url)
            if html is None:
                driver.quit()
                driver = await self.init_driver()
                count = 0
            else:
                break

        await self.driver_pool.put((driver, count+1))
        print(url)
        return html


class Headless2:
    def __init__(self, loop=None, pool_size=10):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.driver_options = webdriver.ChromeOptions()
        self.driver_options.add_argument('headless')

        self.sem = asyncio.Semaphore(pool_size)
        self.service = Service('chromedriver')
        self.service.start()

    async def init_pool(self, pool_size):
        for i in range(pool_size):
            driver = webdriver.Remote(self.service.service_url, desired_capabilities=self.driver_options.to_capabilities())
            await self.driver_pool.put(driver)

    async def get(self, url, locator=None, timeout=10):

        def _get(_driver, _url):
            wait = WebDriverWait(_driver, timeout, poll_frequency=1)
            try:
                _driver.delete_all_cookies()
                _driver.get(_url)
                if locator:
                    wait.until(EC.presence_of_all_elements_located(locator))
                else:
                    wait.until(lambda x: x.execute_script('return document.readyState;') == 'complete')
                return _driver.page_source
            except Exception as e:
                print(url, e)

            return None

        async with self.sem:
            print(url)
            driver = webdriver.Remote(self.service.service_url,
                                        desired_capabilities=self.driver_options.to_capabilities())
            html = await _get(driver, url)
            driver.quit()
            print(url)
            return html



