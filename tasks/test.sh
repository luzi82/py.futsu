#!/bin/bash
set -e

. secret/env.sh
pytest
