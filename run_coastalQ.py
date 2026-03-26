#!/usr/bin/env python3
"""
Author : Kyle Wright
Date   : March, 2026
Purpose: Delta Discharge Partitioning
"""

import glob
import json
from pathlib import Path
import argparse
import numpy as np
import xarray as xr
from coastalQ import DeltaPartition

# Build command format:
# docker build --quiet -f {repo_path}/Dockerfile -t {docker_username}/coastalq:{tag_name} {repo_path}/coastalQ
# Run command format:
# docker run -v {mnt_dir}:/mnt/ {docker_username}/coastalq:{tag_name} --mntdir /mnt -r /mnt/input/reaches.json --index 0

# algorithms from which to partition discharge, formatting information copied from consensus scripts
algo_metadata = {
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
    }
}

def run_coastwide(mntdir, reachfile=None):
    """
    Main method to execute coastalQ class methods.

    Parameters
    ----------
    mntdir (Path): Path to mount directory containing input and output folders
    reachfile (str): Optional path to JSON file containing list of reach IDs by which to filter

    Returns
    -------
    None (saves partitioned discharge files to disk)
    """

    # local directory containing delta network data
    delta_network_dir = Path('/app/coastalQ') / 'delta_networks'
    outdir = mntdir / 'data' / 'coastalq'

    # Load apex reaches metadata (maps delta_name to reach IDs)
    with open(Path('/app/coastalQ') / 'apex_reaches.json') as fp:
        delta_metadata = json.load(fp)

    # Determine which deltas to process
    if reachfile:
        delta_names = filter_deltas_by_reaches(reachfile, delta_metadata)
    else:
        delta_names = [delta['delta_name'] for delta in delta_metadata]
    
    # Loop through deltas
    for name in delta_names:
        # initialize list to hold discharge arrays for each algorithm
        delta_inflows = []
        algorithm_names = []
        
        # instantiate delta object and load edge weights from disk
        delta_partition = DeltaPartition(delta_name=name, output_dir=outdir)
        delta_partition.load_edge_weights(delta_network_dir)

        # find SWORD apex reach for this delta
        inflow_ids = [r['apex_sword_ids'] for r in delta_metadata if r['delta_name'] == name][0]
        delta_partition.assign_inlets(inflow_ids)
        
        # loop through algorithms
        for algo, metadata in algo_metadata.items():
            # look for output files for this algorithm in the expected location
            infolder = mntdir / 'data' / 'flpe' / algo
            files = glob.glob(str(infolder / '*.nc'))
            if len(files) == 0:
                continue # skip if no files for this algorithm

            try:
                # grab discharge data for the relevant reaches from the algorithm folder
                delta_inflows.append(delta_partition.combine_and_clean_discharge(infolder, algo, metadata))
                algorithm_names.append(algo)
            except Exception as e:
                print(f"Error processing delta {name} for algorithm {algo}: {e}")
                continue
        
        if not delta_inflows:
            print(f"No algorithm data found for delta {name}, skipping")
            continue
        
        # concatenate inflows from different algorithms along new 'algos' dimension, dropping time steps with all NaNs
        delta_inflow = xr.concat(delta_inflows, dim='algos', join='outer')
        delta_inflow = delta_inflow.dropna(dim='time', how='all')

        # partition discharge to sub-reaches
        delta_discharge = delta_partition.partition_discharge(delta_inflow.values)
        time_since = delta_partition.time_to_epoch(delta_inflow['time'])
        
        # save in outdir as deltaname.nc, e.g. "mississippi.nc"
        delta_partition.save_partitioned_discharge(algorithms = algorithm_names)
        print(f"Successfully processed delta {name}")
    return

def filter_deltas_by_reaches(reachfile, delta_metadata):
    """
    Helper function to filter deltas based on reaches of interest.
    
    Parameters
    ----------
    reachfile (str): path to JSON file containing list of reach IDs to filter by
    delta_metadata (list): list of dicts containing delta metadata, including apex reach IDs
    
    Returns
    ----------
    set of delta names that have apex reaches matching the reach IDs in the reachfile
    """
    with open(reachfile) as fp:
        reaches = json.load(fp)
    reach_ids_in_file = set(r['reach_id'] for r in reaches)
    
    deltas_to_process = set()
    for delta in delta_metadata:
        if set(delta['apex_sword_ids']).issubset(set(reach_ids_in_file)):
            deltas_to_process.add(delta['delta_name'])
    
    return deltas_to_process

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mntdir", type=str, default="/mnt", help="Mount directory.")
    parser.add_argument("-i", "--index", help="Range of indices (Not used, included for consistency).")
    parser.add_argument("-r", "--reachfile", type=str, default="/mnt/data/input/reaches.json", help="Reach JSON file.")
    args = parser.parse_args()
    run_coastwide(Path(args.mntdir), args.reachfile)
