"""
The target dataset: catalyst particle
Root folder: /gdata/RAVEN/lamni/2020-1/comm_33IDD
This script is used for reconstructing with rpie
There is a companion reader script: catalyst_reader_helper.py
"""

import os

import click
import numpy as np
import skimage.io
import skimage.restoration
import tike.ptycho
import tike.ptycho.io
import tike.ptycho.object
import shutil
import logging
import socket
import os.path
from os import path
import catalyst_reader_helper as crh

logger = logging.getLogger(__name__)

_options_converter = {
  'rpie': tike.ptycho.RpieOptions,
  'cgrad':tike.ptycho.CgradOptions
}


@click.command()
@click.option('--overwrite', is_flag=True, 
              help='Clears the output directory before writing data.')
@click.option('--niter', default=100, type=click.INT,
              help='Number of iterations.')
@click.option('--output-freq', default=10, type=click.INT,
              help='Output frequency. Images are stored after every 10 iterations, if this is set to 10.')
@click.option('--batch-size', default=10, type=click.INT,
              help='Batch size, default is 10.')
@click.option('--ifile', type=click.STRING,
              help='Input diffraction patterns\' master file.')
@click.option('--algorithm', type=click.STRING, default='rpie',
              help='Algorithm for reconstructions:(rpie, cgrad, lstsq_grad)')
@click.option('--log-file', type=click.STRING, default='tike.log',
              help='Target log file.')
@click.option( '--folder', default='.', 
    type=click.Path(exists=False, file_okay=False, dir_okay=True,
                    writable=True, readable=True, resolve_path=True),
    help='A folder where the output is saved.')
@click.option('--gpu-id', default=0, type=click.INT,
              help='GPU id to pin this cupy. Currently, if this is set then ngpu has to be 1.')
def main(
    algorithm,
    niter,
    batch_size,
    ifile,
    output_freq,
    overwrite, 
    folder,
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
    logging.info(f"{hostname}: Passed parameters: \n"
                 f"ifile:{ifile}; ofile:{folder};\n"
                 f"algorithm:{algorithm}; iterations:{niter};\n"
                 f"output_freq:{output_freq}; overwrite:{overwrite};\n"
                 f"log_file:{log_file}; gpu_id:{gpu_id};\n")

    logging.info(f"Begin read: data, scan, probe")
    reader = crh.CatalystReader(ifile)#.read_catalyst_data_range()
    dall = reader.read_catalyst_data(250) # read single data
    logging.info(f"data shape: {dall['data'].shape}; scan shape: {dall['positions_real'].shape}; probe shape:{dall['initprobe'].shape}")
    scan, data, probe = reader.normalize_catalyst_data(dall)
    logging.info(f"normalized/reduced data shape: {data.shape}; scan shape: {scan.shape}; probe shape:{probe.shape}")
    logging.info(f"File was read.")

    assert probe.ndim == 5
    assert scan.ndim == 2
    assert data.ndim == 3

    # Below changes the scan positions and generates an object
    (psi, scan) = tike.ptycho.object.get_padded_object(scan, probe)

    result = dict(
      scan=scan,
      probe=probe,
      psi=psi,
      object_options=tike.ptycho.ObjectOptions(),
      probe_options=tike.ptycho.ProbeOptions(),
      algorithm_options=_options_converter[algorithm](
        num_batch=batch_size,
        num_iter=output_freq)
    )

    ### Checking output folder ###
    if os.path.exists(folder) and overwrite: 
      logging.info(f"Output folder exists; removing: {folder}")
      shutil.rmtree(folder)
    if not os.path.exists(folder): 
      logging.info(f"Creating output folder: {folder}")
      os.makedirs(folder)
    np.save(f'{folder}/scan-{algorithm}-000.npy', result['scan'])
    ### End of output folder check ###


    logging.info(f"Starting iterations.")
    for i in range(output_freq, niter + output_freq, output_freq):
      result = tike.ptycho.reconstruct(
        **result,
        data=data,
        num_gpu=(gpu_id,) # It is important to pass (gpu_id,) instead of (gpu_id), otherwise tuple converts to integer
      )

      # Save the object to disk
      phase = np.angle(result['psi'])
      ampli = np.abs(result['psi'])
      # phase = skimage.restoration.unwrap_phase(phase).astype('float32')
      skimage.io.imsave(f'{folder}/O-{algorithm}-{i:03d}.tiff',
                        phase.astype('float32'))
      skimage.io.imsave(f'{folder}/A-{algorithm}-{i:03d}.tiff',
                        ampli.astype('float32'))

      # Save the probe to disk
      for m in range(probe.shape[-3]):
        skimage.io.imsave(
            f'{folder}/P-{probe.shape[-3]}-{m}-{algorithm}-{i:03d}.tiff',
            np.square(np.abs(probe[..., m, :, :])).astype('float32'))

    logging.info(f"End of iterations.")

if __name__ == '__main__':
    main()
