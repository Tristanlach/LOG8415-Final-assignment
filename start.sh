#!/bin/bash

echo "Creating the infrastructure..."

# Check if python3 exists and use it, otherwise fallback to python
if command -v python3 &>/dev/null; then
    python3 iac.py python3
elif command -v python &>/dev/null; then
    python iac.py python
else
    echo "Python is not installed. Please install Python to proceed."
    exit 1
fi
