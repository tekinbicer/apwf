# coding: utf-8
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

def pi(num_points):
    from random import random
    inside = 0
    for i in range(num_points):
        x, y = random(), random()  # Drop a random point in the box.
        if x**2 + y**2 < 1:        # Count points within the circle.
            inside += 1
    return (inside*4 / num_points)
    
theta_endpoint = 'f765db7a-038c-47ea-9176-d81de31c054f'

pi_function = fxc.register_function(pi)

res = fxc.run(10**5, endpoint_id=theta_endpoint, function_id=pi_function)
