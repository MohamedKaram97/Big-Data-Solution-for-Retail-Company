import os
import shutil
import time

def move_one_subfolder_per_hour(source_folder, target_folder):
    """Moves one subfolder per hour from source to target."""

    subfolders = [f for f in os.listdir(source_folder) if os.path.isdir(os.path.join(source_folder, f))]
    num_subfolders = len(subfolders)

    for i, subfolder in enumerate(subfolders):
        subfolder_path = os.path.join(source_folder, subfolder)
        target_subfolder_path = os.path.join(target_folder, subfolder)

        try:
            shutil.move(subfolder_path, target_subfolder_path)
            print(f"Moved '{subfolder}' from '{source_folder}' to '{target_folder}'")
        except Exception as e:
            print(f"Error moving '{subfolder}': {e}")

        # Calculate remaining time until the next hour
        if i < num_subfolders - 1:  # Avoid unnecessary sleep after the last subfolder
            current_time = time.localtime()
            seconds_to_next_hour = 3600 
            print(f"Waiting {seconds_to_next_hour} seconds until the next hour...")
            time.sleep(seconds_to_next_hour)  # Wait until the next hour

if __name__ == "__main__":
    source_folder = r"D:\ITI\24-Spark\Project\Spark Project\data"
    target_folder = r"D:\ITI\15-Hadoop\spark-sql-and-pyspark-using-python3\data\project"

    move_one_subfolder_per_hour(source_folder, target_folder)
    print("All subfolders moved successfully!")
