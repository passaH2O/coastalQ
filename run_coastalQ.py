#!/usr/bin/env python3
"""
Author : Kyle Wright
Date   : March, 2026
Purpose: Delta Discharge Partitioning
"""

import os
import glob
import json
from pathlib import Path
import argparse
import numpy as np
import xarray as xr
from netCDF4 import Dataset
from coastalQ import DeltaPartition

# algorithms from which to partition discharge
ALGO_METADATA = {
    'momma': {
        'qvar':'Q',
        'time':'time_str'
    },
    'hivdi': {
        'qvar':'reach/Q',
        'time':'time'
    },
    'neobam':{
        'qvar':'q/q',
        'time':'time_str'
    },
    'metroman':{
        'qvar':'average/allq',
        'time':'time_str'
    },
    'sic4dvar':{
        'qvar':'Q_da',
        'time':'times'
    },
    'sad':{
        'qvar':'Qa',
        'time':'time_str'
    },
    'consensus':{
        'qvar':'consensus_q',
        'time':'time_str'
    # },
    # 'integrator':{
    #     'qvar':'q_u',
    #     'time':'time_str'
    # },
    # 'offline':{
    #     'qvar':'q_u',
    #     'time':'time_str'
    }
}

mntdir = Path('C:/Users/kwright/Documents/Projects/ETH_SWOT_Confluence/coastalQ') # to be replaced with actual mount directory when running in workflow, e.g. "/mnt"
OUTDIR = mntdir / 'data' / 'coastalq'
# def run_coastwide(mntdir, index=None, reachfile=None):
#     """Main method to execute coastalQ class methods."""

# local directory containing delta network data
DELTA_NETWORK_DIR = mntdir / 'coastalQ' / 'delta_networks'
DELTA_NAMES = next(os.walk(DELTA_NETWORK_DIR))[1] # just directory names
APEX_REACHES = json.load(open(mntdir / 'coastalQ' / 'apex_reaches.json'))

for ALGO, metadata in ALGO_METADATA.items():
    # infolder = mntdir / 'flpe' / ALGO
    infolder = Path(r'C:/Users/kwright/Documents/Projects/ETH_SWOT_Confluence/SWOT-Confluence-Offline/confluence_withDelta/withDelta_mnt') / 'flpe' / ALGO
    
    files = glob.glob(str(infolder / '*.nc'))
    if len(files) == 0:
        continue # skip if no files for this algorithm
    
    for NAME in DELTA_NAMES:
        try:
            delta_partition = DeltaPartition(delta_name=NAME, output_dir=OUTDIR)
            delta_partition.load_edge_weights(DELTA_NETWORK_DIR)

            # find SWORD apex reach for this delta
            inflow_ids = [r['apex_sword_ids'] for r in APEX_REACHES if r['delta_name'] == NAME][0]
            delta_partition.assign_inlets(inflow_ids)
            
            # to-do: grab discharge for that reach from earlier modules
            apex_discharge = np.array([5, 100, 2000]).astype(np.float32) # placeholder

            delta_discharge = delta_partition.partition_discharge(apex_discharge)

            # save in outdir as deltaname_algorithm.nc, e.g. "mississippi_consensus.nc"
            delta_partition.save_partitioned_discharge(algorithm_name=ALGO) # to-do: loop over algorithm names and save for each
        
        except Exception as e:
            print(f"Error processing delta {NAME} for algorithm {ALGO}: {e}")
            continue


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--mntdir", type=str, default="/mnt", help="Mount directory.")
#     parser.add_argument("-i", "--index", type=parse_range)#, required=True)
#     parser.add_argument("-r", "--reachfile", type=str, default="reaches.json", help="Reach JSON file.")
#     args = parser.parse_args()
#     run_coastwide(Path(args.mntdir), args.index, args.reachfile)
