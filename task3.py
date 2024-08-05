# Import library
import os
import time
import dask
import dask.dataframe as dd
from distributed import Client
import subprocess
from memory_profiler import profile

# Processing task 3 to summing account balance and sorting for top 1000000 account_id
# @profile to memory usage monitoring, turn off @profile to speed up computation time
# @profile
def process_task_3(file_list, csv_file_path):
    """
    Processes multiple large CSV files in parallel using Dask DataFrames to do task 3.
    Args:
        file_list: List of file paths to the CSV files.
    """
    # Import data
    ddf = dd.read_csv(file_list,blocksize="10MB")

    # Grouping account balance summing per account_id
    ddf = ddf.groupby('account_id').sum(split_out=1000).reset_index()

    # Checking descriptive statistic of data to do filtering
    # print(ddf.describe().compute())
    # From describe before we know that top 1 million is around 0.4% of total data (around 250 million)
    # ddf = ddf[ddf.account_balance > 1000000]
    # After filtering with account balance > 100000, we got data around 2 million row with around 1.8 million as median
    # Filtering with account balance > 1.7 million will get data around top 1 million to reduce complexity
    ddf = ddf[ddf.account_balance > 1700000]

    # Verify know we have data around 1 million
    # print(ddf.describe().compute())

    # Write output to the csv file
    ddf.to_csv(csv_file_path, index=False, single_file=True)

    # Using unix shell command to sorting highest to lowest and top 1 million richest accounts
    command_1 = "tail -n+2 ./task_3_result.csv | sort -t ',' -nk2 -r > ./tmp.csv"
    command_2 = "head -1000000 ./tmp.csv > ./task_3_result.csv"
    command_3 = "sed -i '1 i account_id,account_balance' ./task_3_result.csv"
    command_4 = "rm ./tmp.csv"

    subprocess.run(command_1, capture_output=False, shell=True)
    subprocess.run(command_2, capture_output=False, shell=True)
    subprocess.run(command_3, capture_output=False, shell=True)
    subprocess.run(command_4, capture_output=False, shell=True)

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

    # Process task 3 to get csv file
    csv_file_path = os.getcwd() + "/task_3_result.csv"
    process_task_3(file_list, csv_file_path)

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