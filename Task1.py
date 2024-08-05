# Import library
import os
import time
import dask
import dask.dataframe as dd
from distributed import Client
from memory_profiler import profile

# Processing task 1 to summing all account balance across all csv file using dask dataframe
# @profile to memory usage monitoring, turn off @profile to speed up computation time
# @profile
def process_task_1(file_list):
    """
    Processes multiple large CSV files in parallel using Dask DataFrames to do task 1.
    Args:
        file_list: List of file paths to the CSV files.
    """
    # Import data
    ddf = dd.read_csv(file_list)

    # Calculate sum of account balance
    balance = ddf.account_balance.sum().compute()

    # Write output to the txt file
    with open('task_1_result.txt', 'w') as f:
        f.write(str(balance))

# Get all file paths in directory
def get_file_paths(directory):
    """
    Getting all file path in directories.
    Args:
        directory: source of all desired file path.
    """
    # List to store the file paths
    file_paths = []

    # Using os.walk() to get all directories and subdirectories
    for root, directories, files in os.walk(directory):
        for filename in files:
            # Join root path and file name each file and appeend to storing file paths
            file_path = os.path.join(root, filename)
            file_paths.append(file_path)
    return file_paths

if __name__ == "__main__":
    # Defining start time for monitoring time usage
    st = time.time()
    st_cpu = time.process_time()

    # Connect to dask client and setting memory limit with 1GB
    client = Client(memory_limit="1GB")

    # Get all file path in data directory
    file_list = get_file_paths("./data")

    # Process task 1 to get txt file which contain summing of all account balance
    process_task_1(file_list)

    # Defining stop time for monitoring time usage
    et = time.time()
    et_cpu = time.process_time()

    # Defining difference of start time and stop time for monitoring time usage
    elapsed_time = et - st
    res = et_cpu - st_cpu

    # Print execution time and cpu execution time
    print('Execution time:', elapsed_time, 'seconds')
    print('CPU Execution time:', res, 'seconds')

    # Close dask client
    client.close()