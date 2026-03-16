#!/usr/bin/env python3
"""
Author : Kyle Wright
Date   : March, 2026
Purpose: Delta Discharge Partitioning
"""

import os
import glob
import json
import numpy as np
from netCDF4 import Dataset
from coastalQ import DeltaPartition

# local directory containing delta network data
delta_networks_directory = os.path.join(os.getcwd(), 'coastalQ', 'delta_networks')
delta_names = next(os.walk(delta_networks_directory))[1] # just directory names
apex_reaches = json.load(open(os.path.join(os.getcwd(), 'coastalQ', 'apex_reaches.json')))

for delta in delta_names:
    # wrap in try/except to allow for missing delta metadata
    delta_partition = DeltaPartition(delta_ID=delta)
    delta_partition.set_width_adjacency()
    delta_partition.build_routing_vector()

    # find SWORD apex reach for this delta
    apex_reach = [r['apex_reach_id'] for r in apex_reaches if r['delta_name'] == delta][0]
    # to-do: grab discharge for that reach from earlier modules
    # to-do: likely need to loop over options for discharge inputs, consensus, metroman, etc. and run partitioning for each

    delta_discharge = delta_partition.partition_discharge(discharge=1000) # dummy discharge value for now, to be replaced with actual discharge time series in future
    

# def main():
#     """Main method to execute coastalQ class methods."""

# if __name__ == "__main__":
#     main()
