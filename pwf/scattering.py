import unittest
import sys
import time
from unittest.result import failfast
import yaml
import uuid
import pathlib
import logging

from funcx.sdk.client import FuncXClient

# Parameters for the function execution
# Below funcx endpoints are under Tekin's accound and 
# needs to be setup for individual user.
FX_ENDPOINT_UUID = '2feb2ed7-32ec-426f-9d0c-f85f2ef324a2' # Theta KNL endpoint
#FX_ENDPOINT_UUID = '037523e6-0f96-4df6-b2a4-b37d2877bbbe' # LCRC Bebop endpoint
NPI_RPC = 128 # Number of pi functions to be run.
wtime = 2 # Wait time for the 
counter=0
################


# Define funcX test functions
def hworld():
    import socket
    hello_host= f"{socket.gethostname()}: hello world"
    return hello_host

def pi(num_points, divider):
    from random import random
    inside = 0
    for i in range(num_points):
        x, y = random(), random()  # Drop a random point in the box.
        if x**2 + y**2 < 1:        # Count points within the circle.
            inside += 1
    return (inside*4 / num_points) / divider


fxc = FuncXClient()
fxc.version_check()


ep_info = fxc.get_endpoint_status(FX_ENDPOINT_UUID)
if ep_info['status'] != 'online': 
    raise RuntimeError("End point is not online!")


pi_fuid = fxc.register_function(pi, description=f"Test function to calculate pi.")


# Create a batch of pi function tasks
npi_batch = fxc.create_batch()
for i in range(NPI_RPC):
    npi_batch.add(10**8, i+1, endpoint_id=FX_ENDPOINT_UUID, function_id=pi_fuid)

batch_task_ids = fxc.batch_run(npi_batch)
while True: 
    batch_task_status = fxc.get_batch_result(batch_task_ids)
    finished_tasks = [ s for s in batch_task_status if batch_task_status[s]['status'] == 'success']
    for task in finished_tasks: print(f"Finished task: {task}: {batch_task_status[task]}")
    running_tasks = [ s for s in batch_task_status if batch_task_status[s]['status'] != 'success']
    if not running_tasks: break
    else: 
        print(f"Total exec. time (sec): {counter}; Tasks are still running: {running_tasks}")
    time.sleep(wtime) # Wait 2 secs before checking the results
    counter = counter+wtime
