#!/usr/bin/env python3
"""
Runner script for the modular pipeline.

This script demonstrates how to run the modular pipeline from the parent directory
and provides various usage examples.
"""

import sys
import os

# Add the modular_pipeline directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modular_pipeline'))

# Import and run the main function
from modular_pipeline.main import main

if __name__ == "__main__":
    print("🚀 Running Modular Query Analysis Pipeline")
    print("=" * 50)
    main()