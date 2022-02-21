import unittest
import sys
import time
from unittest.result import failfast
import yaml
import uuid
import pathlib
import logging

from funcx.sdk.client import FuncXClient

# Define funcX test functions
def pi(num_points=10**8):
    from random import random
    inside = 0
    for i in range(num_points):
        x, y = random(), random()  # Drop a random point in the box.
        if x**2 + y**2 < 1:        # Count points within the circle.
            inside += 1
    return (inside*4 / num_points)

def hworld():
    import socket
    hello_host= f"{socket.gethostname()}: hello world"
    return hello_host
# End of funcX test functions' definition


class TestFuncXEndpoints(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #print("Setting up test class")
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


    def test_check_endpoints(self):
        for ep_key in self.funcx_endpoints:
            ep_uuid = self.funcx_endpoints[ep_key]
            ep_info = self.fxc.get_endpoint_status(ep_uuid)
            self.assertEqual(ep_info['status'], 'online', f"Endpoint is not online: {ep_key}:{ep_uuid}")
    

    def test_register_functions(self) -> tuple[str, dict]:
        test_funcs_desc_uuid = str(uuid.uuid4()) # random uuid for test functions

        try:
            test_funcs = {
                'pi' : self.fxc.register_function(pi, description=f"Test function to calculate pi. UUID={test_funcs_desc_uuid}"),
                'hworld' : self.fxc.register_function(hworld, description=f"Test function to get hostname of the worker. UUID={test_funcs_desc_uuid}")
            }
        except BaseException as e:
           self.fail(f"FuncX function registration is failed: {e}")

        return test_funcs_desc_uuid, test_funcs


    @unittest.skip("This is not a critical test and can fail because of the delayed function registration.")
    def test_fx_search_funcs(self):
        test_funcs_desc_uuid, _ = self.test_register_functions()

        # Search indexing might take some time after function registration.
        time.sleep(self.funcx_req_delay_time)

        search_results = self.fxc.search_function(f"{test_funcs_desc_uuid}", offset=0, limit=5)
        self.assertTrue(len(search_results) == 2, f"There should be exactly two functions.")


    @unittest.skip("This can take time to run.")
    def test_fx_batch_run(self):
        log= logging.getLogger("PWF.ServiceTests.funcX")

        _ , test_funcs = self.test_register_functions()

        test_func_batch = self.fxc.create_batch()
        for ep_key in self.funcx_endpoints:
            ep_uuid = self.funcx_endpoints[ep_key]
            for fx_test_key in test_funcs:
                fx_test_uuid = test_funcs[fx_test_key]
                test_func_batch.add(endpoint_id=ep_uuid, function_id=fx_test_uuid)

        # Initiate test functions' batch run and receive task ids
        batch_task_ids = self.fxc.batch_run(test_func_batch)

        # Get the initial status of the tasks
        counter=0
        while True: 
            batch_task_status = self.fxc.get_batch_result(batch_task_ids)
            running_tasks = [ s for s in batch_task_status if batch_task_status[s]['status'] != 'success']
            for task in batch_task_status: print(batch_task_status[task])
            if not running_tasks: break
            else: 
                log.info(f"Tasks are still running: {running_tasks}")
            time.sleep(self.funcx_req_delay_time)
            counter = counter+self.funcx_req_delay_time
            self.assertTrue(self.funcx_resource_timeout > (counter/60), f"Funcx functions couldn't finish in set timeout range: {self.funcx_resource_timeout} mins.")


    def tearDown(self):
        return #super().tearDown()
    

    @classmethod
    def tearDownClass(self):
        return



if __name__ == '__main__':
    logging.basicConfig( stream=sys.stderr )
    logging.getLogger("PWF.ServiceTests.funcX").setLevel(logging.DEBUG)

    unittest.main()