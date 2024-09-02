#!/bin/bash

# Activate conda
source /home/vhg/miniconda3/bin/activate

# Activate the specific conda environment
conda activate wackerwetter

# Run your script or command
python /home/vhg/repos/wackerwetter/saveforecast.py

# Optional: Deactivate the conda environment after the script is executed
conda deactivate
