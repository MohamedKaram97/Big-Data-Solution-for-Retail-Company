import os
import subprocess
from datetime import datetime
import shutil

# Get current day name and hour
day_name = datetime.now().strftime('%A')
full_date = datetime.now().strftime('%Y_%m_%d')
hour = datetime.now().strftime('%H')

# Define paths
local_path = "/data/project/"
hdfs_path = "/project/raw_data"
archive_path = os.path.join(local_path, "archive", full_date)

# Iterate over folders in the local path
for folder in os.listdir(local_path):
    full_path = os.path.join(local_path, folder)
    
    if folder != "archive" and os.path.isdir(full_path):
        for root, _, files in os.walk(full_path):

            csv_files = [f for f in files if f.endswith(".csv")]
            if len(csv_files) == 3:

                os.makedirs(archive_path, exist_ok=True)

                # Create HDFS raw data path if it does not exist
                hdfs_mkdir_cmd = f"/opt/hadoop/bin/hdfs dfs -mkdir -p {hdfs_path}/{full_date}/{hour}"
                subprocess.run(hdfs_mkdir_cmd, shell=True, check=True)

                # Move Data
                hdfs_cmd = f"/opt/hadoop/bin/hdfs dfs -put {full_path}/*.csv {hdfs_path}/{full_date}/{hour}"
                subprocess.run(hdfs_cmd, shell=True, check=True)
                
                # Move the folder to the archive path in local system
                shutil.move(full_path, archive_path)
                os.rename(archive_path+f"/{folder}", archive_path+f"/{hour}")

                break
                
                
                