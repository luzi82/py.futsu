#!/bin/bash
set -e

. secret/env.sh
python setup.py sdist bdist_wheel
twine upload dist/*
