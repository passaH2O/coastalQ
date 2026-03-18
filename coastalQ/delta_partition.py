#!/usr/bin/env python

import os
import glob
import numpy as np
import pandas as pd
# from netCDF4 import Dataset
import xarray as xr

class DeltaPartition:
    def __init__(self, delta_name, output_dir=None, **kwargs):
        self.delta_name = delta_name.lower() # ensure delta name is lowercase for consistency with metadata
        self.width_adjacency = None
        self.apex_sword_ids = None
        self.output_dir = output_dir

        for key, value in kwargs.items():
            setattr(self, key, value)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def assign_inlets(self, apex_sword_ids):
        """Method to assign SWORD reach IDs for delta inlets (i.e. reaches where discharge is partitioned from) based on metadata in apex_reaches.json file."""
        self.apex_sword_ids = apex_sword_ids
        return

    def set_width_adjacency(self):
        """Placeholder for method to set width adjacency matrix for discharge partitioning in each delta."""
        # Assumed to be loading pre-saved width adjacency matrices for each delta from disk, or (eventually) calculating them from the SWORD river width data.
        return

    def compute_edge_weights(self, width_adjacency=None):
        """
        Take a width-weighed adjacency matrix and convert it to a 1D partitioning vector for all sub-reaches
        
        Parameters
        ----------
        width_adjacency (np.ndarray) : Width-weighted adjacency matrix for some delta network

        Saves
        ----------
        (np.ndarray) Normalized 1D vector of fraction of apex inflow partitioned to each link
        """
        if getattr(self,'width_adjacency') is None:
            self.width_adjacency = width_adjacency

        num_sub_reaches = self.width_adjacency.shape[0] # number of delta channels

        # initialize matrix with normalized discharge of 1
        routing_init = np.zeros((num_sub_reaches, 1))
        routing_init[0, 0] = 1.0

        # distribute width partitioning across network to create 1D vector of total fraction in each link
        I = np.eye(num_sub_reaches)
        transform_matrix = I - self.width_adjacency.T
        self.norm_partitioning = np.linalg.inv(transform_matrix) @ routing_init
        return

    def load_edge_weights(self, networks_directory):
        """
        Load a pre-compiled edge list with width-weighted discharge partitioning computed from RivGraph for each delta.
        
        Parameters
        ----------
        networks_directory (str) : Path to directory containing edge list and weights for each delta,
        saved as {delta_name}_reaches.csv in subdirectory for each delta

        Saves
        ----------
        self.local_reach_IDs (list) : List of local reach IDs for each sub-reach
        
        self.norm_partitioning (np.ndarray) : Normalized 1D vector of fraction of apex inflow partitioned to each link
        """
        reaches = pd.read_csv(os.path.join(networks_directory, self.delta_name, f"{self.delta_name}_reaches.csv"))
        self.local_reach_IDs = reaches['reach_id_R'].to_list()
        self.norm_partitioning = reaches['rg_flux'].to_numpy().astype(float)
        return

    def partition_discharge(self, discharge):
        """
        Method to partition discharge into delta sub-reaches.
        
        Parameters
        ----------
        discharge (np.ndarray) : 1D array of discharge values computed at the delta apex (i.e. from SWOT discharge algorithms)

        Returns
        ----------
        (np.ndarray) 2D array of discharge values partitioned into each delta sub-reach
        """
        if getattr(self, 'norm_partitioning', None) is None:
            self.load_edge_weights() # make sure routing vector is loaded
        if isinstance(discharge, (float, int)):
            discharge = np.array([discharge]) # convert to np.ndarray if not already

        # partition discharge according to routing vector
        self.sub_reach_discharge = discharge[:,np.newaxis] * self.norm_partitioning

        return self.sub_reach_discharge

    def save_partitioned_discharge(self, algorithm_name):
        """
        Method to save partitioned discharge as NetCDF file.

        Parameters
        ----------
        algorithm_name (str) : Name of discharge algorithm used to compute discharge at delta apex, to be included in output filename and metadata

        Saves
        ----------
        NetCDF file in output directory with filename {delta_name}_{algorithm_name}.nc, e.g. "mississippi_consensus.nc"
        """
        
        xrds = xr.Dataset(
            coords={
                # 'nt' : (['nt'], ),
                'reach' : (['reach'], np.array(self.local_reach_IDs).astype(int))
            },
            data_vars = {
                'Q' : (['nt','reach'], self.sub_reach_discharge)
            }
        )

        xrds['reach'].attrs = {
            'long_name': 'Local Delta Reach ID',
            'units': '-'
        }

        xrds['Q'].attrs = {
            'standard_name': 'water_volume_transport_in_river_channel',
            'long_name': 'River Discharge',
            'units': 'm3 s-1',
            'valid_min': 0.0,
            '_FillValue': 'NaN',
            'coverage_content_type': 'modelResult',
            'coordinates': 'time reach_id',
            'type' : 'data'
        }

        xrds.attrs = {
            'title': 'SWOT Delta Discharge Product',
            'intitution': 'ETH Zurich, Los Alamos National Lab, Penn State University',
            'source': 'SWOT coastalQ Confluence Module',
            'creator_name': 'Kyle Wright, Eleanor Hensen, Sabrina Ashik, Jon Schwenk, Paola Passalacqua, Anastasia Piliouras',
            'creator_email': 'kwright at ethz.ch',
            'file_type': 'array',
            'Conventions': 'CF-1.10, ACDD-1.3',
            'module_version': 0.0,
            'summary': 'Delta Discharge',
            'history': "None",
            'comment': "None",
            'keywords': 'SWOT, SWORD, Delta, coastalQ, Discharge',
            'license': 'Freely Distributed'
        }

        # save to output directory with filename {delta_name}_{algorithm_name}.nc, e.g. "mississippi_consensus.nc"
        filename = os.path.join(self.output_dir, f"{self.delta_name}_{algorithm_name}.nc")
        xrds.to_netcdf(filename) # Save
        return

    def update_SoS(self):
        """Placeholder for method to update SWOT discharge output files with discharge assigned in the deltas."""
        return