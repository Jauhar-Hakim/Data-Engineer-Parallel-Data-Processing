# Import library
import os
import time
import dask
import dask.dataframe as dd
from distributed import Client
from memory_profiler import profile

# Processing task 4 to calculate sum of balance top 1 million richest accounts and ratio compared to total sum all account
# @profile to memory usage monitoring, turn off @profile to speed up computation time
# @profile
def process_task_4(csv_file,txt_file):
    """
    Processes multiple large CSV files in parallel using Dask DataFrames to do task 4.
    Args:
        file_list: List of file paths to the CSV files.
    """
    # Import data
    ddf = dd.read_csv(csv_file,blocksize="1MB")

    # Calculate sum of account balance
    balance_richest = ddf.account_balance.sum().compute()

    # Read result of sum all account from task_1_result.txt
    with open('task_1_result.txt', 'r') as f:
         balance_all = f.readline()

    ratio = float((float(balance_richest)/float(balance_all))*100)

    # Write output to the txt file
    with open('task_4_result.txt', 'w') as f:
        f.write(str(balance_richest)+"\n")
        f.write(str(ratio))


if __name__ == "__main__":
    # Defining start time for monitoring time usage
    st = time.time()
    st_cpu = time.process_time()

    # Connect to dask client and setting memory limit with 1GB
    client = Client(memory_limit="1GB")

    csv_file = "./task_3_result.csv"
    txt_file = "./task_1_result.txt"

    # Process task 4 to get txt file which contain summing of balance top 1 million richest accounts and ratio compared to sum of balance all account
    process_task_4(csv_file,txt_file)

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