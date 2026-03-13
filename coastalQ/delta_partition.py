#!/usr/bin/env python

import numpy as np
import os
from netCDF4 import Dataset

class DeltaPartition:
    def __init__(self, delta_ID, **kwargs):
        self.delta_ID = delta_ID
        self.width_adjacency = None
        self.inflow_reach_ID = None

        for key, value in kwargs.items():
            setattr(self, key, value)

    def set_width_adjacency(self):
        """Placeholder for method to set width adjacency matrix for discharge partitioning in each delta."""
        # Assumed to be loading pre-saved width adjacency matrices for each delta from disk, or (eventually) calculating them from the SWORD river width data.
        # Dummy data for now
        self.width_adjacency = np.array([
            [0, 0.6, 0.4, 0,   0  ],
            [0, 0,   0,   0.5, 0.5],
            [0, 0,   0,   0,   0  ],
            [0, 0,   0,   0,   0  ],
            [0, 0,   0,   0,   0  ]
        ])
        return

    def build_routing_vector(self, width_adjacency=None):
        """
        Take a width-weighed adjacency matrix and convert it to a 1D partitioning vector for all sub-reaches
        
        Parameters
        ----------
        width_adjacency (np.ndarray) : Width-weighted adjacency matrix for some delta network

        Returns
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

        return self.norm_partitioning

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
            self.build_routing_vector() # make sure routing vector is built
        if isinstance(discharge, (float, int)):
            discharge = np.array([discharge]) # convert to array if single value

        # partition discharge according to routing vector
        self.sub_reach_discharge = discharge[:,np.newaxis] * self.norm_partitioning

        return self.sub_reach_discharge

    def update_SoS(self):
        """Placeholder for method to update SWOT discharge output files with discharge assigned in the deltas."""
        return