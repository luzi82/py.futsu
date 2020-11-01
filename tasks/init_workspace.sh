#!/bin/bash

set -e

python -m pip install --upgrade pip wheel
pip install flake8 pytest
pip install -r requirements.txt
pip install --upgrade setuptools nose twine keyring google-cloud-storage boto3
