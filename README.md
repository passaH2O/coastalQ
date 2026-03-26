# coastalQ
Confluence module for partitioning SWOT discharge in coastal river deltas. 

The current implementation of this module (`v0.0`) propagates all FLPE discharge estimates downstream to the delta apex and applies width-based partitioning across the delta distributary network. Because most deltas are not yet accurately resolved in SWORD, partitioning weights have been computed based on a select set of cleaned delta networks available in the `coastalQ/delta_networks` subdirectory. Outputs are saved into the SoS on a per-delta basis (rather than a SWORD reach-ID basis), which is expected to change in future versions once those reaches are integrated into SWORD. Future versions of this module are expected to incorporate more discharge algorithms, to cover additional deltas, and to account for channel-island connectivity.

Example Docker build and run commands can be found in the comments of `run_coastalQ.py`