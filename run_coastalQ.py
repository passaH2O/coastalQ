#!/usr/bin/env python3
"""
Author : Kyle Wright
Date   : March, 2026
Purpose: Delta Discharge Partitioning
"""

from html import parser
import os
import glob
import json
from pathlib import Path
import argparse
import numpy as np
from netCDF4 import Dataset
from coastalQ import DeltaPartition
from git_repositories.consensus.consensus import parse_range

mntdir = os.getcwd() # to be replaced with actual mount directory when running in workflow, e.g. "/mnt"
OUTDIR = os.path.join(mntdir, 'data', 'coastalq')
# def run_coastwide(mntdir, index=None, reachfile=None):
#     """Main method to execute coastalQ class methods."""

# local directory containing delta network data
DELTA_NETWORK_DIR = os.path.join(mntdir, 'coastalQ', 'delta_networks')
DELTA_NAMES = next(os.walk(DELTA_NETWORK_DIR))[1] # just directory names
APEX_REACHES = json.load(open(os.path.join(mntdir, 'coastalQ', 'apex_reaches.json')))

for NAME in DELTA_NAMES:
    # for algo in {algorithms}
    # wrap in try/except to allow for missing delta metadata
    delta_partition = DeltaPartition(delta_name=NAME, output_dir=OUTDIR)
    delta_partition.load_edge_weights(directory=DELTA_NETWORK_DIR)

    # find SWORD apex reach for this delta
    inflow_ids = [r['apex_sword_ids'] for r in APEX_REACHES if r['delta_name'] == NAME][0]
    delta_partition.assign_inlets(inflow_ids)
    # to-do: grab discharge for that reach from earlier modules
    # to-do: likely need to loop over options for discharge inputs, consensus, metroman, etc. and run partitioning for each

    delta_discharge = delta_partition.partition_discharge(discharge=1000) # dummy discharge value for now, to be replaced with actual discharge time series in future

    # save in outdir as deltaname_algorithm.nc, e.g. "mississippi_consensus.nc"


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--mntdir", type=str, default="/mnt", help="Mount directory.")
#     parser.add_argument("-i", "--index", type=parse_range)#, required=True)
#     parser.add_argument("-r", "--reachfile", type=str, default="reaches.json", help="Reach JSON file.")
#     args = parser.parse_args()
#     run_coastwide(Path(args.mntdir), args.index, args.reachfile)
