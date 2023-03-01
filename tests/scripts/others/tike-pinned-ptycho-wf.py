"""Reconstruct some ptychography data using tike version
0b989e31d6227d6d37f56661c99fed8237f26f35 or equivalent.
"""

import logging
import sys
import shutil
import cupy
import re
import pynvml as pm
from os import path
import random
import numpy as np
import skimage.restoration
import skimage.io
import tike.ptycho
import tike.ptycho.io
import tike.ptycho.object
import click
import time
import socket
import fcntl
import os

logger = logging.getLogger(__name__)

_options_converter = {
  'rpie': tike.ptycho.RpieOptions
}


def list_of_avail_gpus(mem_threshold):
  pm.nvmlInit()

  ngpus = pm.nvmlDeviceGetCount()
  ghandles = []
  for i in range(ngpus):
    handle = pm.nvmlDeviceGetHandleByIndex(i)
    ghandles.append(handle) 

  m_infos = []
  for handle in ghandles:
    #m_infos[0].total; m_infos[0].free; m_infos[0].used;
    m_infos.append(pm.nvmlDeviceGetMemoryInfo(handle)) 

  p_infos = {}
  for i in range(len(ghandles)):
    proc_infos = pm.nvmlDeviceGetComputeRunningProcesses(ghandles[i])
    p_infos[i] = {}
    for proc_info in proc_infos:
      #p_infos[gpud_id][pid]="process-name";
      p_infos[i][proc_info.pid] = pm.nvmlSystemGetProcessName(proc_info.pid)
 
  avail_gpus = []
  for i in range(len(ghandles)):
    p_info = p_infos[i]
    m_info = m_infos[i]
    if(len(p_info)==0 and # there is no process running
        (m_info.used//(mem_threshold * 2**20)) < mem_threshold): # the memory utilization is smaller than 50MiB
      avail_gpus.append(i)

  return avail_gpus


def getgpus(ngpus, strgpus):
  ls = list(strgpus)
  gpus = []
  total=0
  for index in range(len(ls)):
    if ls[index] != '-': 
      gpus.append(ls[index])
      ls[index]='-'
      total = total+1
    if total==ngpus: break
  return (list(map(int, gpus)), ''.join(ls))


def get_avail_gpu(ngpus):
  sync_file = '/dev/shm/gpulockfile'
  time.sleep(random.uniform(0.1,1.0))
  if path.exists(sync_file): #this is still not race-free
    a = open(sync_file, 'r+')
  else:
    a = open(sync_file, 'a+')
  while True: 
    fcntl.lockf(a, fcntl.LOCK_EX) #lock is here
    a.seek(0)
    strgpus = a.read(10)
    if len(strgpus) == 0:
      strgpus = '01'
    mtuple = getgpus(ngpus, strgpus)
    if len(mtuple[0]) != ngpus: 
      fcntl.lockf(a, fcntl.LOCK_UN)
      time.sleep(2)
    else: break

  a.seek(0)
  a.write(mtuple[1])
  fcntl.lockf(a, fcntl.LOCK_UN)
  a.close()
  return mtuple[0]

def release_gpus(gpus):
  sync_file = '/dev/shm/gpulockfile'
  a = open(sync_file, 'r+')
  fcntl.lockf(a, fcntl.LOCK_EX)
  a.seek(0)
  strgpus = a.read(10)

  ls = list(strgpus)
  for gpu_id in gpus:
    ls[gpu_id] = str(gpu_id)
  #for index in range(len(ls)):
  #  if len(gpus) == 0 : break
  #  if ls[index] == '-': 
  #    ls[index]=str(gpus.pop())
  a.seek(0)
  a.write(''.join(ls))
  fcntl.lockf(a, fcntl.LOCK_UN)
  a.close()


@click.command()
@click.option('--use-mpi', is_flag=True, 
              help='Use mpi for multi-process/node multi-gpu configurations.')
@click.option('--recover-probe', is_flag=True, 
              help='Recover probe.') 
@click.option('--recover-psi', is_flag=True,
              help='Recover object.') 
@click.option('--overwrite', is_flag=True, 
              help='Clears the output directory before writing data.')
@click.option('--ngpu', default=1, type=click.INT,
              help='Number of GPUs to use for reconstruction.')
@click.option('--auto-pin', is_flag=True,
              help="Automatically check GPUs for availability and pin to the free GPU. "
                   "There should be no active process on the GPU and the memory utilization "
                   "should be below gpu-mem-threshold to recognize a GPU as available.")
@click.option('--gpu-mem-threshold', default=50, type=click.INT,
              help="Memory constraint for the auto pin functionality. E.g., if this is "
                   "set to 50 (default), GPU memory utilization needs to be smaller than 50MiB.")
@click.option('--niter', default=100, type=click.INT,
              help='Number of iterations.')
@click.option('--output-freq', default=10, type=click.INT,
              help='Output frequency. Images are stored after every 10 iterations, if this is set to 10.')
@click.option('--batch-size', default=120, type=click.INT,
              help='Batch size, default is 120.')
@click.option('--model', default='gaussian', type=click.STRING,
              help='Noise model for cost function.')
@click.option('--ifile', type=click.STRING,
              #default='/projects/hp-ptycho/bicer/ptycho/2021-2/commission/ptycho/fly001/fly001_master.h5',
              help='Input diffraction patterns\' master file.')
@click.option('--position-path', type=click.STRING,
              #default='/projects/hp-ptycho/bicer/ptycho/2021-2/commission/positions/fly001_0.txt',
              help='Corresponding positions file of the ifile.')
@click.option('--probe-path', type=click.STRING,
              default='/projects/hp-ptycho/bicer/ptycho/2021-2/commission/probes/velociprobe-probe.npy',
              help='Probe initialization file.')
@click.option('--algorithm', type=click.STRING, default='lstsq_grad',
              help='Algorithm for reconstructions:(cgrad, lstsq_grad)')
@click.option('--nmodes', type=click.INT, default=1,
              help='Number of probe modes.')
@click.option('--recover-positions', is_flag=True,
              help='Whether or not to correct positions.')
@click.option('--log-file', type=click.STRING, default='tike.log',
              help='Target log file.')
@click.option( '--folder', default='.', 
    type=click.Path(exists=False, file_okay=False, dir_okay=True,
                    writable=True, readable=True, resolve_path=True),
    help='A folder where the output is saved.')
@click.option('--gpu-id', default=None, type=click.INT,
              help='GPU id to pin this cupy. Currently, if this is set then ngpu has to be 1.')
#@click.option('--dpcenter', nargs=2, default=None, type=click.INT,
#              help='Center of the diffraction in (y,x) format. E.g., for (y=259,x=541), --dpcenter 259 541')
#@click.option('--dpsize', default=128, type=click.INT,
#              help="Diffraction pattern crop size. Given the dpcenter, "
#              "the diffraction pattern is cropped +-dpsize. Default is 128.")
#@click.option('--fids', type=click.STRING, default=None,
#              help='Unique file ids to reconstruct in "001,002,.." format. E.g., --fids=\'001,002\'. Default is None, i.e., reconstruct all files.')
def main(
    # NOTE: Seems that cgrad method is broken; only the lstsq_grad method works
    algorithm,#='lstsq_grad',
    niter,
    batch_size,
    ifile,
    position_path,
    probe_path,
    output_freq,
    ngpu,
    auto_pin, 
    gpu_mem_threshold, 
    model, 
    use_mpi, 
    recover_probe, 
    recover_psi, 
    overwrite, 
    nmodes, 
    folder,
    recover_positions, 
    log_file,
    gpu_id
):

    logging.basicConfig(
      filename=log_file,
      filemode='a',
      format='%(asctime)s %(levelname)s %(message)s',
      level=logging.INFO,
      datefmt='%Y-%m-%d %H:%M:%S',
    )

    hostname = socket.gethostname()

    logging.info(f"Passed parameters: \n"
                 f"ifile:{ifile}; ofile:{folder};\n"
                 f"algorithm:{algorithm}; iterations:{niter};\n"
                 f"output_freq:{output_freq}; ngpu:{ngpu};\n"
                 f"model:{model}; use_mpi:{use_mpi};\n"
                 f"recover_probe:{recover_probe}; recover_psi:{recover_psi};\n"
                 f"overwrite:{overwrite}; nmodes:{nmodes};\n"
                 f"recover_positions:{recover_positions};\n"
                 f"auto_pin:{auto_pin}; gpu_mem_threshold:{gpu_mem_threshold};\n"
                 f"log_file:{log_file}; gpu_id:{gpu_id};\n"
                 f"hostname:{hostname}\n")

    ### Pin this task to an available GPU
    logging.info("Starting autopinning...")
    try:
      if auto_pin:
        avail_gpus = get_avail_gpu(1)
        if len(avail_gpus) > 0:
          if (gpu_id is None) or (gpu_id not in avail_gpus):
            gpu_id = avail_gpus[0]
          #cupy.cuda.Device(gpu_id).use()
          logging.info(f"(Auto) pinned task to GPU:{gpu_id}")
        else: 
          logging.error(f"No GPU is available, unable to pin task.")
          sys.exit(-1)

      if (not auto_pin) and (gpu_id is not None):
        avail_gpus = list_of_avail_gpus(gpu_mem_threshold)
        if ngpu > 1:
          logging.error(f"GPU id is set to {gpu_id}, currently the number of GPUs cannot be larger than 1.")
          sys.exit(-2)
        if gpu_id not in avail_gpus:
          logging.error(f"gpu_id:{gpu_id} is not available. Available gpus:{avail_gpus}.")
          sys.exit(-3)
        #cupy.cuda.Device(gpu_id).use()
        logging.info(f"Pinned task to GPU:{gpu_id}")
    except BaseException as e:
      logging.info(f"Failed: {e}")
      raise e

    logging.info("Ending autopinning...")
    ### End auto_pin ###

    logging.info(f"Task will be pinned to GPU: {gpu_id}")

    ### Reading the data files
    logging.info(f"Begin read: data, scan, probe")
    data, scan = tike.ptycho.io.read_aps_velociprobe(
      diffraction_path=ifile,
      position_path=position_path,
      # NOTE: Used non-default columns because this data is irregular and
      # only has encoder positions no interferrometer positions.
      xy_columns=(5, 6),
    )
    # NOTE: Because detector distance has wrong units in these datasets we must
    # multiply by 1000 to convert from mm to meters
    scan *= 1000

    # NOTE: For unknown reasons, only one of the position axes is inverted.
    scan[..., 1] = -scan[..., 1]

    # NOTE: This initial probe guess was provided by Yi Jiang and then
    # converted from MATLAB to NumPy format. It is a (12, 256, 256) array
    # representing 12 velociprobe probes.
    probe = np.load(probe_path)[:nmodes]
    probe = probe[np.newaxis, np.newaxis, ...]
    # probe = np.swapaxes(probe, -2, -1)
    probe = probe.astype(np.complex64)

    assert probe.ndim == 5
    assert scan.ndim == 2
    assert data.ndim == 3

    logging.info(f"End read: data, scan, probe")
    logging.info(f"Probe: shape={probe.shape}; type={probe.dtype}")
    logging.info(f"Positions: shape={scan.shape}; type={scan.dtype}")
    logging.info(f"Data: shape={data.shape}; type={data.dtype}")

    # Below changes the scan positions and generates an object
    (psi, scan) = tike.ptycho.object.get_padded_object(scan, probe)

    result = dict(
      scan=scan,
      probe=probe,
      psi=psi,
      object_options=tike.ptycho.ObjectOptions(),
      probe_options=tike.ptycho.ProbeOptions(),
      algorithm_options=_options_converter[algorithm](
        num_batch=10,
        num_iter=output_freq,
      ),
    )

    ### Checking output folder ###
    if os.path.exists(folder) and overwrite: 
      logging.info(f"Output folder exists; removing: {folder}")
      shutil.rmtree(folder)
    if not os.path.exists(folder): 
      logging.info(f"Creating output folder: {folder}")
      os.makedirs(folder)
    np.save(f'{folder}/scan-{nmodes}-{algorithm}-000.npy', result['scan'])
    ### End of output folder check ###


    logging.info(f"Starting iterations.")
    for i in range(output_freq, niter + output_freq, output_freq):
      result = tike.ptycho.reconstruct(
        **result,
        data=data,
        num_gpu=(gpu_id%2,), # It is important to pass (gpu_id,) instead of (gpu_id), otherwise tuple converts to integer
      )

      # Save the object to disk
      phase = np.angle(result['psi'])
      ampli = np.abs(result['psi'])
      # phase = skimage.restoration.unwrap_phase(phase).astype('float32')
      skimage.io.imsave(f'{folder}/O-{nmodes}-{algorithm}-{i:03d}.tiff',
                        phase.astype('float32'))
      skimage.io.imsave(f'{folder}/A-{nmodes}-{algorithm}-{i:03d}.tiff',
                        ampli.astype('float32'))

      # Save the probe to disk
      for m in range(probe.shape[-3]):
        skimage.io.imsave(
            f'{folder}/P-{probe.shape[-3]}-{m}-{algorithm}-{i:03d}.tiff',
            np.square(np.abs(probe[..., m, :, :])).astype('float32'))


    logging.info(f"End of iterations.")

    if auto_pin: release_gpus(avail_gpus)


if __name__ == '__main__':
  main()
