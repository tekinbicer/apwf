# FuncX function definition for remote DAQ machine directory calls
def get_folder_paths(path):
    import glob
    import re

    return sorted(glob.glob(path, recursive=False),
                key = lambda v : int(re.search(r"(\d+)" , v[len(v)-"".join(reversed(v)).index('/'):]).group(0)))


def get_file_paths(path):
    import glob
    import re

    return sorted(glob.glob(path, recursive=False),
                key = lambda v : int(re.search(r"(\d+)" , v[len(v)-"".join((reversed(v))).index('/'):v.index('.')]).group(0)))

def isdir(path):
    import os
    return os.path.isdir(path)

def isfile(path):
    import os
    return os.path.isfile(path)

def daq_wf_prepare(input_folder_path, position_folder_path, output_folder_path, 
                    probe_file_path=None,
                    create_output_folder=True, cleanup_output_folder=False):
    '''Check and prepare the data acquisition machine's folders for workflow
    execution.

    The workflows system requires all the folders to be ready before its
    execution. This function checks if the inputs and their folders are ready.
    It also can create and delete the output folders, which is its responsibility
    to manage. 

    Args:
        input_folder_path:
            A string that points to the diffraction patterns' directory path.
            The scan data is stored to under this folder. The workflow system
            expects this folder to exist, otherwise it returns an error code 
            (1).
        position_folder_path:
            A string that points to the position directory path. The positions
            of the scan data are stored under this folder. The workflow system
            expects this folder to exist, otherwise it returns an error code
            (2).
        probe_file_path:
            A string that points to the probe file (used for initializing probe.)
            If this is set, the system checks file existance and returns an error
            code (5) in case not found. 
        output_folder_path:
            A string that points to the analyzed/reconstructed images directory
            path. The workflow system expects this folder to exist, if the
            folder does not exist then it can create the folder after checking
            create_output_folder, otherwise it returns an error message (error
            code: 3). If it cannot create output folder, then it returns an
            error code (4).  If the folder exists and cleanup_output_folder is
            set, it removes all folders and files under this folder.
    
    Returns:
        0 if all is fine; otherwise, see the error codes above.
    '''
    import os
    from shutil import rmtree

    if not os.path.isdir(input_folder_path): return 1
    if not os.path.isdir(position_folder_path): return 2
    if probe_file_path is not None:
        if not os.path.isfile(probe_file_path): return 5

    check_output_folder = os.path.isdir(output_folder_path)
    if (not check_output_folder):
        if not create_output_folder: return 3
        else:
            try: os.makedirs(output_folder_path, exist_ok=True)
            except Exception as e: return 4
    # All folders are ready.
    
    if cleanup_output_folder:
        for f in os.listdir(output_folder_path):
            full_path = f"{output_folder_path}/{f}"
            if os.path.isfile(full_path): os.remove(full_path)
            if os.path.isdir(full_path): rmtree(full_path, ignore_errors=True)

    return 0



 