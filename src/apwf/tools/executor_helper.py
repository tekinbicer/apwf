import sys
import csv
from dataclasses import InitVar, dataclass
from dataclasses import field
import datetime
import re
import logging

@dataclass(frozen=True)
class EConfigs:
    src_config: InitVar[dict]
    dest_config: InitVar[dict]
    add_larger_than: int  = -1

    src_wf_root_path: str = field(init=False)
    src_input_folder_prefix: str = field(init=False)
    src_input_pos_folder_prefix: str = field(init=False)
    src_output_folder_prefix: str = field(init=False)
    dest_wf_root_path: str = field(init=False)
    dest_input_folder_prefix: str = field(init=False)
    dest_output_folder_prefix: str = field(init=False)

    def __post_init__(self, src_config, dest_config):
        object.__setattr__(self, 'src_wf_root_path', src_config['root_folder_path'])
        #self.src_wf_root_path = src_config['root_folder_path']
        object.__setattr__(self, 'src_input_folder_prefix', src_config['input_folder_prefix'])
        object.__setattr__(self, 'src_input_pos_folder_prefix', src_config['input_position_folder_prefix'])
        object.__setattr__(self, 'src_output_folder_prefix', src_config['output_folder_prefix'])

        object.__setattr__(self, 'dest_wf_root_path', dest_config['root_folder_path'])
        object.__setattr__(self, 'dest_input_folder_prefix', dest_config['input_folder_prefix'])
        object.__setattr__(self, 'dest_output_folder_prefix', dest_config['output_folder_prefix'])


@dataclass(frozen=True)
class WFPaths:
    wfc: InitVar[EConfigs]
    src_input_folder_paths: InitVar[list[str]]
    diff_iids: InitVar[list[str]]

    plist: list = field(init=False)

    def __post_init__(self, wfc, src_input_folder_paths, diff_iids):
        fsrc_input_folder_paths = []
        src_input_pos_files = []
        src_output_folder_paths = []
        dest_output_folder_paths = []
        dest_input_folder_paths = []
        dest_input_pos_files = []
        iids = []

        for src_input_folder_path in src_input_folder_paths[:-1]:
            iid = re.findall(r'\d+', src_input_folder_path)

            # Skip if iid is already activated or processed
            if (iid[-1] not in diff_iids): continue

            iids.append(iid[-1])
            fsrc_input_folder_paths.append(src_input_folder_path)
            src_input_pos_files.append(f"{wfc.src_wf_root_path}/{wfc.src_input_pos_folder_prefix}/fly{iid[-1]}_0.txt")
            src_output_folder_path = f"{wfc.src_wf_root_path}/{wfc.src_output_folder_prefix}/{iid[-1]}"
            src_output_folder_paths.append(src_output_folder_path)
            dest_input_folder_path = f"{wfc.dest_wf_root_path}/{wfc.dest_input_folder_prefix}/{iid[-1]}"
            dest_input_folder_paths.append(dest_input_folder_path)
            dest_input_pos_files.append(f"{dest_input_folder_path}/fly{iid[-1]}_0.txt")
            dest_output_folder_path = f"{wfc.dest_wf_root_path}/{wfc.dest_output_folder_prefix}/{iid[-1]}"
            dest_output_folder_paths.append(dest_output_folder_path)

        iids.reverse()
        fsrc_input_folder_paths.reverse()
        src_input_pos_files.reverse()
        src_output_folder_paths.reverse()
        dest_input_folder_paths.reverse()
        dest_input_pos_files.reverse()
        dest_output_folder_paths.reverse()

        object.__setattr__(self, 'plist', list(zip(iids, 
                fsrc_input_folder_paths, src_input_pos_files, src_output_folder_paths, 
                dest_input_folder_paths, dest_input_pos_files, dest_output_folder_paths)))
        
        apwfLogger = logging.getLogger('apwf')
        if apwfLogger.isEnabledFor(logging.DEBUG):
            for (iid, src_input_folder_path, src_input_pos_file, src_output_folder_path, 
                dest_input_folder_path, dest_input_pos_file, dest_output_folder_path ) in self.plist:
                apwfLogger.debug(f"{iid}: Source input folder: {src_input_folder_path}; "
                            f"Source position file: {src_input_pos_file}; "
                            f"Source output folder: {src_output_folder_path}")
                apwfLogger.debug(f"Dest. input folder: {dest_input_folder_path}; "
                            f"Dest. position file: {dest_input_pos_file}; "
                            f"Dest. output folder: {dest_output_folder_path}")
            if(len(iids)>0):
                apwfLogger.debug(f"All iids:{iids}\n"
                                #f"Processed iids from output folder:{oiids}\n"
                                #f"Currently/actively being processed iids:{activated_iids}\n"
                                f"Process scans with iid>{wfc.add_larger_than}\n"
                                f"To be processed iids:{diff_iids}")
    

@dataclass(frozen=True)
class WFFlowsInputs:
    src_endpoint: InitVar[str]
    dest_endpoint: InitVar[str]
    compute_fx_endpoint: InitVar[str]
    func_ptycho_uuid: InitVar[str]
    task_config: InitVar[dict]
    exec_params_config: InitVar[dict]
    sample_name: InitVar[str]
    paths: InitVar[list]

    filist: list[dict] = field(default_factory=list, init=False) 

    def __post_init__(self, src_endpoint, dest_endpoint, compute_fx_endpoint, 
                        func_ptycho_uuid, task_config, exec_params_config, 
                        sample_name, paths):
        gcounter = 0
        fitmp = []
        for (iid, src_input_folder_path, src_input_pos_file, src_output_folder_path,
            dest_input_folder_path, dest_input_pos_file, dest_output_folder_path ) in paths:

            flow_input = {
                "iid" : iid,
                "input": {
                    "source_endpoint": f"{src_endpoint}",
                    "source_path": f"{src_input_folder_path}/",
                    "source_pos_path": f"{src_input_pos_file}",
                    "dest_endpoint": dest_endpoint,
                    "dest_path": f"{dest_input_folder_path}/",
                    "dest_pos_path": dest_input_pos_file,

                    "result_path": f"{dest_output_folder_path}",
                    "source_result_path": f"{src_output_folder_path}",
                    "fx_ep": f"{compute_fx_endpoint}",
                    "fx_id": f"{func_ptycho_uuid}",
                    "params": {'ifpath': f"{dest_input_folder_path}/fly{iid}_master.h5",
                            'ofpath': f"{dest_output_folder_path}/",
                            'position_path': dest_input_pos_file,
                            'script_path': task_config['executable']['script_path'],
                            'python_path': task_config['python_path'],
                            **exec_params_config,
                            'sample_name': sample_name, #wf_config['flow']['sample_name'],
                            'wid':gcounter}
                }
            }
            gcounter+=1
            fitmp.append(flow_input)
        object.__setattr__(self, 'filist', fitmp)

@dataclass(frozen=True)
class WFGladierFlowsInputs:
    src_endpoint: InitVar[str]
    dest_endpoint: InitVar[str]
    compute_fx_endpoint: InitVar[str]
    func_ptycho_uuid: InitVar[str]
    task_config: InitVar[dict]
    exec_params_config: InitVar[dict]
    sample_name: InitVar[str]
    paths: InitVar[list]

    filist: list[dict] = field(default_factory=list, init=False) 

    def __post_init__(self, src_endpoint, dest_endpoint, compute_fx_endpoint, 
                        task_config, exec_params_config, 
                        sample_name, paths):
        gcounter = 0
        fitmp = []
        for (iid, src_input_folder_path, src_input_pos_file, src_output_folder_path,
            dest_input_folder_path, dest_input_pos_file, dest_output_folder_path ) in paths:

            flow_input = {
                "iid" : iid,
                "input": {
                    # Transfer inputs
                    "source_endpoint": f"{src_endpoint}",
                    "source_path": f"{src_input_folder_path}/",
                    "source_pos_path": f"{src_input_pos_file}",
                    "dest_endpoint": dest_endpoint,
                    "dest_path": f"{dest_input_folder_path}/",
                    "dest_pos_path": dest_input_pos_file,

                    "result_path": f"{dest_output_folder_path}",
                    "source_result_path": f"{src_output_folder_path}",
                    # FuncX inputs
                    # "fx_id": f"{func_ptycho_uuid}", GladierTool already knows this(?)
                    "fx_ep": f"{compute_fx_endpoint}",
                    "data": {'ifpath': f"{dest_input_folder_path}/fly{iid}_master.h5",
                            'ofpath': f"{dest_output_folder_path}/",
                            'position_path': dest_input_pos_file,
                            'script_path': task_config['executable']['script_path'],
                            'python_path': task_config['python_path'],
                            **exec_params_config,
                            'sample_name': sample_name, #wf_config['flow']['sample_name'],
                            'wid':gcounter}
                }
            }
            gcounter+=1
            fitmp.append(flow_input)
        object.__setattr__(self, 'filist', fitmp)



def setup_logger(wf_config):
    # Log configuration for both stdout and file
    log_file_name = f"{wf_config['executor']['log_folder_path']}/funcx-ptycho-wf-{wf_config['flow']['sample_name']}.log"
    logFormatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    apwfLogger = logging.getLogger('apwf')
    apwfLogger.setLevel(logging.INFO)

    fileHandler = logging.FileHandler(filename=log_file_name, mode='a')
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.INFO)
    apwfLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging.INFO)
    apwfLogger.addHandler(consoleHandler)

    return apwfLogger


def append_activated_iids(data, csv_path):
    with open(csv_path, 'a', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow([row, datetime.datetime.now()])
            
def read_activated_iids(csv_path, ids_only=True):
    vals = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader: vals.append(row)

    if ids_only:
        activated_iids = []
        for r in vals: activated_iids.append(r[0])
        vals = activated_iids

    return vals

def get_unprocessed_iids(activated_iids, src_input_folder_paths, ex_src_output_folder_paths, only_larger_than=0):
    iids = []
    for src_input_folder_path in src_input_folder_paths:
        iid = re.findall(r'\d+', src_input_folder_path)
        if int(iid[-1]) > only_larger_than:
            iids.append(iid[-1])
        else: continue

    oiids = []
    for ex_src_output_folder_path in ex_src_output_folder_paths:
        oiid = re.findall(r'\d+', ex_src_output_folder_path)
        if int(oiid[-1]) > only_larger_than: 
            oiids.append(oiid[-1])
        else: continue

    # Find the iids that need to be processed
    diff_iids = []
    for iid in iids:
        if (int(iid)>only_larger_than) and (iid not in oiids) and (iid not in activated_iids): 
            diff_iids.append(iid) # process only these
    
    return diff_iids



def executor_wf_prepare(activated_iid_file_path, log_folder_path,
                    create_activated_iid_file=True, create_log_folder=True,
                    cleanup_activated_iid_file=False, cleanup_log_folder=False):
    '''Check and prepare the executer's files and folders for workflow
    execution.

    The workflows system requires all the folders to be ready before its
    execution. This function checks if the inputs and their folders for executor
    are ready. It also can create and cleanup the files, which are its
    responsibility to manage. 

    Args:
        activated_iid_file_path:
            A string that points to the active workflows. The executro
            continuously checks the generated folders (scans) at the data
            acquisition machine.  At any given time, a folder/flow can be in
            processed state that shows processing is done; not-processed state
            that shows the system hasn't started processing the flow, and active
            state that shows the flow is dispatched but not finalized yet. The
            active state information is tracked by a file that was pointed by
            this path.
        log_folder_path:
            A string that points to the log directory path.  The workflow system
            expects this folder to exist, if the folder does not exist then it
            can create the folder after checking create_log_folder. If it
            doesn't exist and create_log_folder is set to False, then it returns
            an error code (4). If the folder exists and cleanup_log_folder is
            True, this function deletes everything under it.
        create_activated_iid_file:
            If activated_iid_file_path does not exist and this flag is set, then
            it is created. Expects target folder to be existed. Otherwise returns
            error code (2).
        create_log_folder:
            If folder at log_folder_path does not exist and this flag is set,
            then the folder is created.
        cleanup_activated_iid_file:
            It this flag is set the content of the activate_iid_file_path is
            cleaned. 
        cleanup_log_folder:
            If this flag is set the files and folders under log_folder_path is
            deleted.
    
    Returns:
        0 if all is fine; otherwise, see the error codes above.
    '''
    import os
    from shutil import rmtree

    # Handle log folder
    check_log_folder = os.path.isdir(log_folder_path)
    if (not check_log_folder):
        if not create_log_folder: return 4
        else:
            try: os.makedirs(log_folder_path, exist_ok=True)
            except Exception as e: return 5
    if cleanup_log_folder:
        for f in os.listdir(log_folder_path):
            full_path = f"{log_folder_path}/{f}"
            if os.path.isfile(full_path): os.remove(full_path)
            if os.path.isdir(full_path): rmtree(full_path, ignore_errors=True)

    # Handle activated_iid_file 
    check_activate_iid_file = os.path.isfile(activated_iid_file_path)
    if not check_activate_iid_file: # file does not exist
        if not create_activated_iid_file: return 1
        else:
            try: open(activated_iid_file_path, 'a').close()
            except Exception as e: 
                print(e)
                return 2
    else:
        if cleanup_activated_iid_file:
            try: 
                os.remove(activated_iid_file_path)
                open(activated_iid_file_path, 'a').close()
            except Exception as e: return 3


    return 0