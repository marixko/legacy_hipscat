
import os
import json
import logging
import warnings
from pathlib import Path
from astropy.table import Table
from astropy.utils.exceptions import AstropyWarning
# Ignore all Astropy warnings
warnings.simplefilter('ignore', category=AstropyWarning)
from dask.distributed import Client
from hipscat_import.pipeline import pipeline_with_client
from hipscat_import.catalog.arguments import ImportArguments


def setup_logging(log_dir, log_file):
    """
    Set up logging configuration.
    
    Parameters:
    log_dir (str or Path): The directory where the log file will be saved.
    log_file (str): The name of the log file.
    """

    logging.basicConfig(filename= os.path.join(log_dir, log_file), level=logging.ERROR,
                        format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(config_file):
    """
    Load configuration from a JSON file.
    
    Parameters:
    config_file (str or Path): The path to the configuration file.
    
    Returns:
    dict: The configuration dictionary.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

def get_parent_directory(path):
    """
    Get the parent directory of the given path.
    
    Parameters:
    path (str or Path): The path to get the parent directory of.
    
    Returns:
    Path: The parent directory.
    """
    return Path(path).resolve().parent


def remove_columns(file_path, columns_to_remove="DCHISQ", save=True):
    """
    Remove a column or a list of columns from an Astropy Table.
    
    Parameters:
    file_path (str): The path to the file containing the Astropy Table.
    columns_to_remove (str or list of str): The column name or list of column names to remove.
    save (bool): If True, save the table to a file. Default is False.

    Returns:
    Table: A new Astropy Table with the specified columns removed.
    """
    try:
        data = Table.read(file_path)

        if isinstance(columns_to_remove, str):
            columns_to_remove = [columns_to_remove]
        
        for column in columns_to_remove:
            if column in data.colnames:
                data.remove_column(column)
            else:
                print(f"Column '{column}' not found in the table. Skipping removal.")

        if save:
            data.write(file_path, overwrite=True, format="fits")
        else:
            return data
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        pass


def process_all_files_in_folder(folder_path, columns_to_remove="DCHISQ", save=True, log_file='failed_files_in_processing.log'):
    """
    Apply remove_columns_from_table function to all files in a folder.
    
    Parameters:
    folder_path (str): The path to the folder containing the files.
    columns_to_remove (str or list of str): The column name or list of column names to remove.
    save (bool): If True, save the modified tables to the same file paths. Default is False.
    """


    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_name.endswith(('.fits')):
            print(f"Processing file: {file_path}")
            remove_columns(file_path, columns_to_remove, save)


def main(input_path, output_artifact_name, output_path):
    """
    Creates hipscat splitting to fits files

    Parameters
    ----------
    input_path (str) : Path to the folder containing fits files
    output_artifact_name (str) : Name of folder for artifacts
    output_path (str) : Path to the output folder
    """

    args = ImportArguments(
    ra_column="RA",
    dec_column="DEC",
    input_path=input_path,
    file_reader="fits",
    output_artifact_name=output_artifact_name,
    output_path=output_path
)
    
    with Client(n_workers=10, memory_limit="8GB") as client:
        pipeline_with_client(args, client)

if __name__ == '__main__':
    # Load configuration
    config_file = Path(__file__).resolve().parent.parent / 'config' / 'config.json'
    config = load_config(config_file)
    
    file_processing_config = config['file_processing']
    logging_config = config['logging']
    hipscat_config = config['hipscat']

    # Resolve paths relative to the script location
    script_dir = Path(__file__).resolve().parent
    legacy_fits_path = (script_dir / file_processing_config['legacy_fits_path']).resolve()
    columns_to_remove = file_processing_config['columns_to_remove']
    log_directory = (script_dir / logging_config['log_directory']).resolve()
    log_file = logging_config['log_file_processing']

    input_path = hipscat_config['input_path']
    output_path = hipscat_config['output_path']
    output_artifact_name = hipscat_config['output_artifact_name']

    setup_logging(log_directory, log_file)

    process_all_files_in_folder(legacy_fits_path, columns_to_remove, save=True)

    main(input_path, output_artifact_name, output_path)

