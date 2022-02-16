import os
import subprocess
import logging
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




class DispatcherHelperFuncs:
    def __init__(self, executable_script_path, python_path="python", log_file_path="funcx-ptycho.log"):
        self.python_path = python_path
        self.executable_script_path = executable_script_path
        
        logging.basicConfig(filename=log_file_path,
                            filemode='a',
                            format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.INFO,
                            datefmt='%Y-%m-%d %H:%M:%S')

    @staticmethod
    def ptycho(**data):
        """Test the ptycho tool"""
        from apwf.tools.fx_comp_helper import DispatcherHelperFuncs

        python_path = data['python_path']
        script_path = data['script_path']
        
        return DispatcherHelperFuncs(python_path, script_path).execute(data)

    def execute(self, **data):
        cmd = self.generate_terminal_str(data)
        logging.info(f"Generated terminal command: {cmd}")

        res = self.exec_terminal_call_str(cmd)
        return res.stdout

    
    def exec_terminal_call_str(self, cmd):
        try:
            res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE,
                                shell=True, executable='/bin/bash')
        except Exception as e:
            logging.error(f"Exception is thrown: {e}")
        return res


    def generate_terminal_str(self, **data):
        rec_ngpu = data['rec_ngpu']
        wid = data['wid']
        dataset_name = data['dataset_name']

        remote_log_file_name = "/grand/hp-ptycho/bicer/20210723_workflow-Xu/logs/tests/tike-wf-{}-w{}-g{}.log".format(dataset_name, wid, rec_ngpu)

        #recon. script parameters
        ifpath = data['ifpath']
        pospath = data['position_path']
        probepath = data['probe_path']
        ofpath = data['ofpath']
        rec_alg = data['rec_alg']
        rec_nmodes = data['rec_nmodes']
        rec_niter = data['rec_niter']
        rec_output_freq = data['rec_output_freq']
        rec_recover_psi = '--recover-psi' if (('rec_recover_psi' in data) and data['rec_recover_psi']) else ''
        rec_recover_probe = '--recover-probe' if (('rec_recover_probe' in data) and data['rec_recover_probe']) else ''
        rec_recover_positions = '--recover-positions' if (('rec_recover_positions' in data) and data['rec_recover_positions']) else ''
        rec_model = data['rec_model']
        rec_use_mpi = '--use-mpi' if (('rec_use_mpi' in data) and data['rec_use_mpi']) else ''
        rec_overwrite = '--overwrite' if (('rec_overwrite' in data) and data['rec_overwrite']) else ''
        rec_auto_pin = '--auto-pin' if (('rec_auto_pin' in data) and data['rec_auto_pin']) else ''
        rec_gpu_id = data['rec_gpu_id']
        rec_log_filename = remote_log_file_name

        try:
            os.mkdir(ofpath)
        except:
            pass

        cmd = ( f"{self.python_path} {self.script_path} --algorithm={rec_alg} --nmodes={rec_nmodes} "
                f"--niter={rec_niter} --output-freq={rec_output_freq} {rec_recover_psi} "
                f"{rec_recover_probe} {rec_recover_positions} --model={rec_model} "
                f"--ngpu={rec_ngpu} {rec_use_mpi} --ifile='{ifpath}' --position-path={pospath} "
                f"--probe-path={probepath} {rec_overwrite} {rec_auto_pin} --gpu-id={rec_gpu_id} "
                f"--folder='{ofpath}' --log-file='{rec_log_filename}'" )

        return cmd