import csv
import datetime

def append_activated_iids(data, csv_path):
    print("adding row to activated: {}".format(data))
    with open(csv_path, 'a', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow([row, datetime.datetime.now()])
            
def read_activated_iids(csv_path):
    vals = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            vals.append(row)
    return vals


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