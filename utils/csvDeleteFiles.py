import os

dir_path = 'data\mock_output_object_storage' 

if os.path.exists(dir_path) and os.path.isdir(dir_path):
    for filename in os.listdir(dir_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(dir_path, filename)
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Error deleting {file_path}: {str(e)}")
    print(".csv file destruction complete")
else:
    print("Directory does not exist or is not a valid directory path.")
