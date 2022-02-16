####
APWF: Automated Ptychography Workflow for Federated Resources 
####

APWF is a custom workflow for reconstruction of ptychography datasets generated
at Advanced Photon Source (APS), Argonne National Laboratory (ANL). 

APWF enables the utilization of remote large-scale compute resources (or
supercomputers) via Globus toolkits; namely, funcX, transfer and automate. All
tools rely on Globus authentication infrastructure (Auth) hence providing single
point of sign-on for execution of data-intensive workflows on supercomputers.

APWF uses Tike toolbox to perform parallel ptychographic reconstruction. Tike is
optimized for multi-node and multi-GPU settings and can perform efficient
reconstruction on high-end GPU resources.