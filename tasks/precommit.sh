#!/bin/bash

flake8 futsu --count --select=E9,F63,F7,F82 --show-source
flake8 futsu --count --max-complexity=10 --max-line-length=127
