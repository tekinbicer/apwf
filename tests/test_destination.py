import unittest
import sys
import time
from unittest.result import failfast
import yaml
import uuid
import pathlib
import logging

from apwf.tools import fx_comp_helper
from funcx.sdk.client import FuncXClient

class TestWFDestination(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.setup_fail_flag = False
        try:
            cls.fxc = FuncXClient()
            cls.fxc.version_check()
        except BaseException as e:
            cls.setup_fail_flag = True
            cls.setup_error_msg = f"Error while setting up funcX client: {e}"
            return

        # Load the configuration file
        try:
            configfile = pathlib.Path(__file__).parent.joinpath("test_wf_config.yml")
            cls.config= yaml.safe_load(open(configfile))
            cls.funcx_endpoints = cls.config['funcx_endpoints']
            cls.funcx_resource_timeout = cls.config['funcx_resource_timeout']
            cls.funcx_req_delay_time= cls.config['funcx_req_delay_time']
        except BaseException as e:
            cls.setup_fail_flag = True
            cls.setup_error_msg = f"Error while setting up funcX configuration file: {e}"
            return
        

    def setUp(self) -> None:
        if self.setup_fail_flag: self.fail(f"Test class initialization error: {self.setup_error_msg}")
        return super().setUp()


    # Tests will go here    



    def tearDown(self):
        return #super().tearDown()
    

    @classmethod
    def tearDownClass(self):
        return