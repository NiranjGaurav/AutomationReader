#!/usr/bin/env python3
"""
Modular Query Analysis Pipeline

A modular version of the complete query analysis pipeline that processes parquet files,
filters PowerBI queries, and analyzes unsupported functions and UDFs.

Modules:
--------
- config: Configuration settings and runtime variables
- data_loader: Parquet file loading functionality
- query_filter: Query filtering and comparison with result files
- function_utils: Utility functions for parsing function lists
- analyzer: Comprehensive analysis of functions and UDFs
- main: Main pipeline orchestration and command-line interface
"""

__version__ = "1.0.0"
__author__ = "Query Analysis Pipeline"

# Import main components
from . import config
from . import data_loader
from . import query_filter
from . import function_utils
from . import analyzer

# Import main functions for easy access
from .data_loader import load_parquet_data
from .query_filter import filter_and_compare
from .analyzer import comprehensive_analysis
