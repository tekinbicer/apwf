from subprocess import PIPE


def ptycho_func(**data):
    """FuncX remote function call that executes Tike script for reconstruction"""
    import os
    import subprocess
    import logging
    from subprocess import PIPE

    remote_log_file_name = f"{data['log_folder_path']}/apwf-{data['sample_name']}-w{data['wid']}.log"
    logging.basicConfig(filename=remote_log_file_name,
                        filemode='a',
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info("FuncX: Starting ptycho funcx function.")
    python_path = data['python_path']
    script_path = data['script_path']

    #task execution parameters
    ifpath = data['ifpath']
    pospath = data['position_path']
    probepath = data['probe_path']
    ofpath = data['ofpath']
    rec_alg = data['algorithm']
    rec_nmodes = data['nmodes']
    rec_niter = data['niter']
    rec_output_freq = data['output_freq']
    rec_recover_psi = '--recover-psi' if (('recover_psi' in data) and data['recover_psi']) else ''
    rec_recover_probe = '--recover-probe' if (('recover_probe' in data) and data['recover_probe']) else ''
    rec_recover_positions = '--recover-positions' if (('recover_positions' in data) and data['recover_positions']) else ''
    rec_model = data['model']
    rec_use_mpi = '--use-mpi' if (('use_mpi' in data) and data['use_mpi']) else ''
    rec_overwrite = '--overwrite' if (('overwrite' in data) and data['overwrite']) else ''
    rec_ngpu = data['ngpus']
    rec_auto_pin = '--auto-pin' if (('auto_pin' in data) and data['auto_pin']) else ''
    rec_log_filename = remote_log_file_name

    try:
        os.mkdir(ofpath)
    except:
        pass

    cmd = ( f"{python_path} {script_path} --algorithm={rec_alg} --nmodes={rec_nmodes} "
            f"--niter={rec_niter} --output-freq={rec_output_freq} {rec_recover_psi} "
            f"{rec_recover_probe} {rec_recover_positions} --model={rec_model} "
            f"--ngpu={rec_ngpu} {rec_use_mpi} --ifile='{ifpath}' --position-path={pospath} "
            f"--probe-path={probepath} {rec_overwrite} {rec_auto_pin} "
            f"--folder='{ofpath}' --log-file='{rec_log_filename}'" )
    logging.info(f"FuncX: Running command: {cmd}")

    try:
        res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE,
                            shell=True, executable='/bin/bash')
    except:
        pass
    outstr = f"{res.stdout}"
    return outstr

def comp_wf_prepare(input_folder_path, output_folder_path, log_folder_path,
                    create_input_folder=True, create_output_folder=True, create_log_folder=True,
                    cleanup_input_folder=False, cleanup_output_folder=False, cleanup_log_folder=False):
    '''Check and prepare the compute cluster's folders for workflow
    execution.

    The workflows system requires all the folders to be ready before its
    execution. This function checks if the inputs and their folders for compute
    tasks are ready. It also can create and cleanup the folders, which is its
    responsibility to manage. 

    Args:
        input_folder_path:
            A string that points to the scan folders' path (root). For each
            folder under this path there are position file, diffraction patterns
            (and others) exist for analysis. The workflow system expects
            this folder. If it doesn't exist and create_input_folder is set to
            False, then it returns an error code (1). If the folder exists and
            cleanup_input_folder is True, this function deletes everything under
            it.
        output_folder_path:
            A string that points to the analyzed/reconstructed images directory
            path. The workflow system expects this folder to exist, if the
            folder does not exist then it can create the folder after checking
            create_output_folder. If it doesn't exist and create_input_folder is
            set to False, then it returns an error code (2). If the folder
            exists and cleanup_output_folder is True, this function deletes
            everything under it.
        log_folder_path:
            A string that points to the log directory path.  The workflow system
            expects this folder to exist, if the folder does not exist then it
            can create the folder after checking create_log_folder. If it
            doesn't exist and create_log_folder is set to False, then it returns
            an error code (3). If the folder exists and cleanup_log_folder is
            True, this function deletes everything under it.
    
    Returns:
        0 if all is fine; otherwise, see the error codes above.
    '''
    import os
    from shutil import rmtree

    # Handle input folder
    check_input_folder = os.path.isdir(input_folder_path)
    if (not check_input_folder):
        if not create_input_folder: return 1
        else:
            try: os.makedirs(input_folder_path, exist_ok=True)
            except Exception as e: return 5
    if cleanup_input_folder:
        for f in os.listdir(input_folder_path):
            full_path = f"{input_folder_path}/{f}"
            if os.path.isfile(full_path): os.remove(full_path)
            if os.path.isdir(full_path): rmtree(full_path, ignore_errors=True)
    
    # Handle output folder
    check_output_folder = os.path.isdir(output_folder_path)
    if (not check_output_folder):
        if not create_output_folder: return 2
        else:
            try: os.makedirs(output_folder_path, exist_ok=True)
            except Exception as e: return 6
    if cleanup_output_folder:
        for f in os.listdir(output_folder_path):
            full_path = f"{output_folder_path}/{f}"
            if os.path.isfile(full_path): os.remove(full_path)
            if os.path.isdir(full_path): rmtree(full_path, ignore_errors=True)

    # Handle log folder
    check_log_folder = os.path.isdir(log_folder_path)
    if (not check_log_folder):
        if not create_log_folder: return 3
        else:
            try: os.makedirs(log_folder_path, exist_ok=True)
            except Exception as e: return 4
    if cleanup_log_folder:
        for f in os.listdir(log_folder_path):
            full_path = f"{log_folder_path}/{f}"
            if os.path.isfile(full_path): os.remove(full_path)
            if os.path.isdir(full_path): rmtree(full_path, ignore_errors=True)

    return 0