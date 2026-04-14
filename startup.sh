#!/bin/bash

# Run chmod +x startup.sh to make this script executable, then run ./startup.sh to set up the environment

# Create a new conda environment with Python 3.12
conda create -n aws python==3.12 -y

# Activate the new environment
conda activate aws

# Install the required packages from requirements.txt
pip install -r requirements.txt
