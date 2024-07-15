import os

def print_directory_structure(root_dir, indent_level=0):
    for item in os.listdir(root_dir):
        if item not in ['logs','.git', '.gitignore','Weather_App_ingestion_Pipeline.egg-info','__pycache__','wx_data']:
            item_path = os.path.join(root_dir, item)
            print('  ' * indent_level + '|-- ' + item)
            if os.path.isdir(item_path):
                print_directory_structure(item_path, indent_level + 1)

# Replace 'your_directory_path' with the path of the directory you want to print
root_directory = r"C:/Users/shubh/shubh/DataEngineeringProjects/Weather-Data-Ingestion-Pipeline"
print_directory_structure(root_directory)
