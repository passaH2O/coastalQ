#!/usr/bin/env python

import os
import numpy as np
import pandas as pd
import xarray as xr

class DeltaPartition:
    """
    Class to perform delta discharge partitioning for a given delta, based on discharge
    at the delta apex and a width-weighted partitioning weights for the delta channel network.
    """
    def __init__(
            self,
            delta_name,
            output_dir=None,
            base_date="2000-01-01T00:00:00Z",
            **kwargs
        ):
        # ensure delta name is lowercase for consistency with metadata
        self.delta_name = delta_name.lower()
        self.width_adjacency = None
        self.apex_sword_ids = None
        self.base_date = pd.Timestamp(base_date) # reference time for output file datetimes
        self.output_dir = output_dir

        for key, value in kwargs.items():
            setattr(self, key, value)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def assign_inlets(self, apex_sword_ids):
        """
        Assign SWORD reach IDs for delta inlet reaches.

        These are reaches where discharge is partitioned from, based on
        metadata in the apex_reaches.json file.
        """
        self.apex_sword_ids = apex_sword_ids
        return

    def set_width_adjacency(self):
        """
        Placeholder for future method to set width adjacency matrix for
        discharge partitioning in each delta.
        """
        # Load pre-saved width adjacency matrices for each delta from disk.
        # Eventually could calculate from SWORD river width data.
        return

    def compute_edge_weights(self, width_adjacency=None):
        """
        Take a width-weighed adjacency matrix and convert it to a 1D partitioning
        vector for all sub-reaches
        
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
        Load a pre-compiled edge list with width-weighted discharge partitioning
        computed from RivGraph for each delta.
        
        Parameters
        ----------
        networks_directory (str) : Path to directory containing edge list and weights
        for each delta, saved as {delta_name}_reaches.csv in subdirectory for each delta

        Saves
        ----------
        self.local_reach_IDs (list) : List of local reach IDs for each sub-reach
        
        self.norm_partitioning (np.ndarray) : Normalized 1D vector of fraction of 
            apex inflow partitioned to each link
        """
        reaches = pd.read_csv(os.path.join(
            networks_directory, self.delta_name, f"{self.delta_name}_reaches.csv"
        ))
        self.local_reach_IDs = reaches['reach_id_R'].to_list()
        self.norm_partitioning = reaches['rg_flux'].to_numpy().astype(float)
        return

    def combine_and_clean_discharge(self, infolder, algo, metadata):
        """
        Read in discharge time series for all SWORD reaches corresponding to the delta
        apex, sum discharge across reaches for each time step, filter out missing data,
        and return cleaned discharge time series to be partitioned in the delta.

        Parameters
        ----------
        infolder (str) : Path to directory containing SWORD discharge output files for
            all reaches, saved as {reach_id}_{algorithm_name}.nc
        algo (str) : Name of the discharge algorithm
        metadata (dict) : Dictionary containing metadata for that algorithm

        Returns
        ----------
        (xr.DataArray) Cleaned discharge time series summed across all apex reaches
            for the delta, with time dimension and any missing data filtered out
        """
        inflow_files = [infolder / f'{reach_id}_{algo}.nc' for reach_id in self.apex_sword_ids]
        qvar = metadata['qvar']
        tvar = metadata['time']
        
        cleaned_datasets = []
        for file in inflow_files:
            # Open the dataset
            if '/' in qvar:
                group,qvar = qvar.split('/')
                dt = xr.open_datatree(file)
                ds = xr.merge([dt.to_dataset(), dt[group].to_dataset()])
            else:
                ds = xr.open_dataset(file)
            
            # for sic4dvar, clean up some dimensions as it causes problems later
            if algo == 'sic4dvar':
                ds = ds.drop_dims('nodes')
                ds = ds.swap_dims({'nt': 'time'})

            # make sense of the time variable
            parsed_times = pd.to_datetime(ds[tvar].values, errors='coerce')
            parsed_times = parsed_times.tz_localize(None).astype('datetime64[ns]') # ensure timezone precision
            ds = ds.assign_coords(time=(ds[tvar].dims, parsed_times))

            # make time main dimension if it's not already
            main_dim = ds[tvar].dims[0]
            if main_dim != 'time':
                ds = ds.swap_dims({main_dim: 'time'})
            
            # filter out any time steps where the time variable is no_data (i.e. invalid)
            valid_time_mask = ds['time'].notnull()
            ds_clean = ds.sel(time=valid_time_mask)

            # append cleaned dataset to list
            cleaned_datasets.append(ds_clean)

        # concatenate datasets along the time dimension, match by calendar day, and sum
        combined_ds = xr.concat(cleaned_datasets, data_vars='all', dim='time')
        combined_ds = combined_ds.sortby('time')
        daily_discharge = combined_ds[qvar].resample(time='1D').sum(dim='time', skipna=True, min_count=1)

        return daily_discharge

    def partition_discharge(self, discharge):
        """
        Method to partition discharge into delta sub-reaches.
        
        Parameters
        ----------
        discharge (np.ndarray) : 1D array of discharge values computed at the
            delta apex (i.e. from SWOT discharge algorithms)

        Returns
        ----------
        (np.ndarray) 2D array of discharge values partitioned into each delta sub-reach
        """
        if getattr(self, 'norm_partitioning', None) is None:
            self.load_edge_weights() # make sure routing vector is loaded
        if isinstance(discharge, (float, int)):
            discharge = np.array([discharge]) # convert to np.ndarray if not already

        # partition discharge according to routing vector
        self.sub_reach_discharge = discharge[:,:,np.newaxis] * self.norm_partitioning

        return self.sub_reach_discharge

    def time_to_epoch(self, times):
        """
        Helper function to convert datetimes to float epoch for saving in NetCDF conventions.

        Parameters
        ----------
        times (np.ndarray) : 1D array of datetimes corresponding to discharge time series

        Returns
        ----------
        (np.ndarray) 1D array of floats corresponding to seconds since base date
        """
        epoch = pd.Timestamp(self.base_date)
        times = pd.to_datetime(times)
        seconds = (times.tz_localize(None) - epoch.tz_localize(None)).total_seconds()
        self.time_since = np.array([float(t) for t in seconds])
        return self.time_since

    def save_partitioned_discharge(self, algorithms):
        """
        Method to save partitioned discharge as NetCDF file.

        Parameters
        ----------
        algorithms (list) : List of discharge algorithm names used to compute discharge
            at delta apex, to be included in output filename and metadata

        Saves
        ----------
        NetCDF file in output directory with filename {delta_name}.nc, e.g. "mississippi.nc"
        """
        
        xrds = xr.Dataset(
            coords={
                'time' : (['nt'], self.time_since),
                'reach' : (['reach'], np.array(self.local_reach_IDs).astype(int)),
                'sword_inlets' : (['sword_inlets'], np.array(self.apex_sword_ids).astype(int)),
                'algos' : (['algos'], algorithms)
            },
            data_vars = {
                'Q' : (['algos','nt','reach'], self.sub_reach_discharge)
            }
        )

        xrds['time'].attrs = {
            'standard_name': 'time',
            'long_name': 'time',
            'units': 'seconds since {t}'.format(t=self.base_date.strftime('%Y-%m-%dT%H:%M:%SZ')),
            'coverage_content_type': 'coordinate',
            'calendar': 'standard',
            'axis': 'T'
        }
        xrds['reach'].attrs = {
            'long_name': 'Local Delta Reach ID',
            'units': '-'
        }
        xrds['sword_inlets'].attrs = {
            'long_name': 'SWORD Reach ID',
            'units': '-'
        }
        xrds['algos'].attrs = {
            'long_name': 'Algorithm Name',
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
            'delta' : self.delta_name,
            'algorithms': ', '.join(algorithms),
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

        # save to output directory with filename {delta_name}.nc, e.g. "mississippi.nc"
        filename = os.path.join(self.output_dir, f"{self.delta_name}.nc")
        xrds.to_netcdf(filename) # Save
        return
