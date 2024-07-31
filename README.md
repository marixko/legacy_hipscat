# Create hipscat catalogues for Legacy DR10

## Overview

This project contains scripts for downloading Legacy DR10 data through web scraping and creating hipscat catalogues for faster crossmatches.

## Configuration

Before running the scripts, make sure to update the configuration file located at config/config.json with the appropriate settings.

### Example Configuration (config/config.json)

```
{
    "file_processing": {
        "legacy_fits_path": "../data/survey/legacy/fits", 
        "columns_to_remove": ["DCHISQ"]
    },

    "logging": {
        "log_directory": "../logs", 
        "log_file_processing": "failed_files_in_processing.log",
        "log_file_crossmatch" : "failes_files_in_crossmatch.log"
    },

    "hipscat":{
        "input_path": "../data/survey/legacy/fits",
        "output_path": "../data/survey/legacy/hipscat",
        "output_artifact_name": "test_legacy"
    }
}
```


## Directory Structure

- config/: JSON Configuration files

- src/: Main source code

- scripts/: Contains scripts for specific tasks

- logs/: Log files


## Setup

1. Clone the repository

2. Install the required packages

3. Update the configuration file config/config.json

4. Run the download_legacy.py script if Legacy DR10 data is not already downloaded

```
> python scripts/download_legacy.py https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr10/south/sweep/10.1/ path/to/output/folder
```

5. Run the legacy_hispcat.py script to create the hipscat catalogues

```
> python scripts/legacy_hipscat.py
```

## Contribute

Please feel free to contribute by submitting a pull request.


